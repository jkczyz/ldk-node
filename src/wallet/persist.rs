// This file is Copyright its original authors, visible in version control history.
//
// This file is licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. You may not use this file except in
// accordance with one or both of these licenses.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use bdk_chain::Merge;
use bdk_wallet::{AsyncWalletPersister, ChangeSet};

use crate::io::utils::{
	read_bdk_wallet_change_set, write_bdk_wallet_change_descriptor, write_bdk_wallet_descriptor,
	write_bdk_wallet_indexer, write_bdk_wallet_local_chain, write_bdk_wallet_network,
	write_bdk_wallet_tx_graph,
};
use crate::logger::{log_error, LdkLogger, Logger};
use crate::types::DynStore;

pub(crate) struct KVStoreWalletPersister {
	latest_change_set: Option<ChangeSet>,
	// Changes handed to the persister but not yet successfully written. Since callers extract
	// staged changes from the wallet *before* persisting (see `Wallet::persist_staged_changes`),
	// the persister has to take over BDK's only-clear-staged-changes-on-success semantics: changes
	// are buffered here and only cleared per-component once the corresponding write succeeds, so a
	// failed or aborted write is retried on the next persist call rather than lost.
	pending_change_set: ChangeSet,
	kv_store: Arc<DynStore>,
	logger: Arc<Logger>,
}

impl KVStoreWalletPersister {
	pub(crate) fn new(kv_store: Arc<DynStore>, logger: Arc<Logger>) -> Self {
		Self { latest_change_set: None, pending_change_set: ChangeSet::default(), kv_store, logger }
	}

	/// Queues the given changes for persistence without writing them out yet. They will be
	/// included in the next [`persist`] call.
	///
	/// This allows callers that already extracted staged changes from the wallet to abort an
	/// operation without losing them.
	///
	/// [`persist`]: Self::persist
	pub(crate) fn stash(&mut self, change_set: ChangeSet) {
		self.pending_change_set.merge(change_set);
	}

	/// Persists the given changes, along with any previously stashed or failed-to-write ones.
	pub(crate) async fn persist(&mut self, change_set: &ChangeSet) -> Result<(), std::io::Error> {
		self.persist_inner(change_set).await
	}

	async fn initialize_inner(&mut self) -> Result<ChangeSet, std::io::Error> {
		// Return immediately if we have already been initialized.
		if let Some(latest_change_set) = self.latest_change_set.as_ref() {
			return Ok(latest_change_set.clone());
		}

		let change_set_opt = read_bdk_wallet_change_set(&*self.kv_store, &*self.logger).await?;

		let change_set = match change_set_opt {
			Some(persisted_change_set) => persisted_change_set,
			None => {
				// BDK docs state: "The implementation must return all data currently stored in the
				// persister. If there is no data, return an empty changeset (using
				// ChangeSet::default())."
				ChangeSet::default()
			},
		};
		self.latest_change_set = Some(change_set.clone());
		Ok(change_set)
	}

	async fn persist_inner(&mut self, change_set: &ChangeSet) -> Result<(), std::io::Error> {
		// Merge the given changes into the pending buffer first and write from there, only
		// clearing each component once its write succeeded. This retains anything that fails (or
		// was previously stashed) for the next persist call.
		self.pending_change_set.merge(change_set.clone());

		if self.pending_change_set.is_empty() {
			return Ok(());
		}

		let kv_store = Arc::clone(&self.kv_store);
		let logger = Arc::clone(&self.logger);

		// We're allowed to fail here if we're not initialized, BDK docs state: "This method can fail if the
		// persister is not initialized."
		let latest_change_set = self.latest_change_set.as_mut().ok_or_else(|| {
			std::io::Error::new(
				std::io::ErrorKind::Other,
				"Wallet must be initialized before calling persist",
			)
		})?;

		// Check that we'd never accidentally override any persisted data if the change set doesn't
		// match our descriptor/change_descriptor/network.
		if let Some(descriptor) = self.pending_change_set.descriptor.clone() {
			if latest_change_set.descriptor.is_some()
				&& latest_change_set.descriptor.as_ref() != Some(&descriptor)
			{
				debug_assert!(false, "Wallet descriptor must never change");
				log_error!(
					logger,
					"Wallet change set doesn't match persisted descriptor. This should never happen."
				);
				return Err(std::io::Error::new(
					std::io::ErrorKind::InvalidData,
					"Wallet change set doesn't match persisted descriptor. This should never happen."
				));
			} else {
				latest_change_set.descriptor = Some(descriptor.clone());
				write_bdk_wallet_descriptor(&descriptor, &*kv_store, Arc::clone(&logger)).await?;
				self.pending_change_set.descriptor = None;
			}
		}

		if let Some(change_descriptor) = self.pending_change_set.change_descriptor.clone() {
			if latest_change_set.change_descriptor.is_some()
				&& latest_change_set.change_descriptor.as_ref() != Some(&change_descriptor)
			{
				debug_assert!(false, "Wallet change_descriptor must never change");
				log_error!(
					logger,
					"Wallet change set doesn't match persisted change_descriptor. This should never happen."
				);
				return Err(std::io::Error::new(
					std::io::ErrorKind::InvalidData,
					"Wallet change set doesn't match persisted change_descriptor. This should never happen."
				));
			} else {
				latest_change_set.change_descriptor = Some(change_descriptor.clone());
				write_bdk_wallet_change_descriptor(
					&change_descriptor,
					&*kv_store,
					Arc::clone(&logger),
				)
				.await?;
				self.pending_change_set.change_descriptor = None;
			}
		}

		if let Some(network) = self.pending_change_set.network {
			if latest_change_set.network.is_some() && latest_change_set.network != Some(network) {
				debug_assert!(false, "Wallet network must never change");
				log_error!(
					logger,
					"Wallet change set doesn't match persisted network. This should never happen."
				);
				return Err(std::io::Error::new(
					std::io::ErrorKind::InvalidData,
					"Wallet change set doesn't match persisted network. This should never happen.",
				));
			} else {
				latest_change_set.network = Some(network);
				write_bdk_wallet_network(&network, &*kv_store, Arc::clone(&logger)).await?;
				self.pending_change_set.network = None;
			}
		}

		debug_assert!(
			latest_change_set.descriptor.is_some()
				&& latest_change_set.change_descriptor.is_some()
				&& latest_change_set.network.is_some(),
			"descriptor, change_descriptor, and network are mandatory ChangeSet fields"
		);

		// Merge and persist the sub-changesets individually if necessary.
		//
		// According to the BDK team the individual sub-changesets can be persisted
		// individually/non-atomically, "(h)owever, the localchain tip is used by block-by-block
		// chain sources as a reference as to where to sync from, so I would persist that last", "I
		// would write in this order: indexer, tx_graph, local_chain", which is why we follow this
		// particular order.
		if !self.pending_change_set.indexer.is_empty() {
			latest_change_set.indexer.merge(self.pending_change_set.indexer.clone());
			write_bdk_wallet_indexer(&latest_change_set.indexer, &*kv_store, Arc::clone(&logger))
				.await?;
			self.pending_change_set.indexer = Default::default();
		}

		if !self.pending_change_set.tx_graph.is_empty() {
			latest_change_set.tx_graph.merge(self.pending_change_set.tx_graph.clone());
			write_bdk_wallet_tx_graph(&latest_change_set.tx_graph, &*kv_store, Arc::clone(&logger))
				.await?;
			self.pending_change_set.tx_graph = Default::default();
		}

		if !self.pending_change_set.local_chain.is_empty() {
			latest_change_set.local_chain.merge(self.pending_change_set.local_chain.clone());
			write_bdk_wallet_local_chain(
				&latest_change_set.local_chain,
				&*kv_store,
				Arc::clone(&logger),
			)
			.await?;
			self.pending_change_set.local_chain = Default::default();
		}

		// We don't persist locked outpoints (mirroring the previous behavior of dropping them), so
		// clear any that were merged in to allow the pending buffer to drain.
		self.pending_change_set.locked_outpoints = Default::default();

		Ok(())
	}
}

impl AsyncWalletPersister for KVStoreWalletPersister {
	type Error = std::io::Error;

	fn initialize<'a>(
		persister: &'a mut Self,
	) -> Pin<Box<dyn Future<Output = Result<ChangeSet, Self::Error>> + Send + 'a>>
	where
		Self: 'a,
	{
		Box::pin(persister.initialize_inner())
	}

	fn persist<'a>(
		persister: &'a mut Self, change_set: &'a ChangeSet,
	) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + 'a>>
	where
		Self: 'a,
	{
		Box::pin(persister.persist_inner(change_set))
	}
}
