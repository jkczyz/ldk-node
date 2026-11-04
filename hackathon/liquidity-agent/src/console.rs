// This file is Copyright its original authors, visible in version control history.
//
// This file is licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. You may not use this file except in
// accordance with one or both of these licenses.

//! Node and demo notices for the operator's terminal. While the model is talking (or the
//! operator is being asked for a decision) the notices are held back, then printed afterwards
//! with repeats collapsed, so they never land in the middle of streamed text.

use std::sync::Mutex;

#[derive(Default)]
pub struct Notices {
	/// `Some` while held; the lines that arrived in the meantime.
	held: Mutex<Option<Vec<String>>>,
}

impl Notices {
	pub fn new() -> Self {
		Self::default()
	}

	/// Prints `line` now, or keeps it until [`Self::release`] if notices are held.
	pub fn note(&self, line: String) {
		match self.held.lock().unwrap().as_mut() {
			Some(held) => held.push(line),
			None => eprintln!("{line}"),
		}
	}

	/// Holds notices until [`Self::release`]. Nested holds are not tracked: the first release
	/// prints.
	pub fn hold(&self) {
		self.held.lock().unwrap().get_or_insert_with(Vec::new);
	}

	/// Prints the held notices, identical lines collapsed into one with a count, and goes back
	/// to printing live.
	pub fn release(&self) {
		let held = self.held.lock().unwrap().take();
		let Some(held) = held else { return };
		if held.is_empty() {
			return;
		}
		eprintln!("\nWhile the model was working:");
		for (line, count) in collapse(held) {
			if count > 1 {
				eprintln!("{line} (x{count})");
			} else {
				eprintln!("{line}");
			}
		}
	}
}

/// Counts identical lines, keeping the order in which each first appeared.
fn collapse(lines: Vec<String>) -> Vec<(String, usize)> {
	let mut collapsed: Vec<(String, usize)> = Vec::new();
	for line in lines {
		match collapsed.iter_mut().find(|(seen, _)| *seen == line) {
			Some((_, count)) => *count += 1,
			None => collapsed.push((line, 1)),
		}
	}
	collapsed
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn collapse_counts_repeats_in_first_seen_order() {
		let lines = ["b", "a", "b", "b", "c", "a"].map(String::from).to_vec();
		let collapsed = collapse(lines);
		let expected = [("b", 3), ("a", 2), ("c", 1)];
		assert_eq!(collapsed.len(), expected.len());
		for ((line, count), (want_line, want_count)) in collapsed.iter().zip(expected) {
			assert_eq!((line.as_str(), *count), (want_line, want_count));
		}
	}

	#[test]
	fn held_notices_are_kept_until_release() {
		let notices = Notices::new();
		notices.hold();
		notices.note("x".into());
		assert_eq!(notices.held.lock().unwrap().as_ref().map(Vec::len), Some(1));
		notices.release();
		assert!(notices.held.lock().unwrap().is_none());
	}
}
