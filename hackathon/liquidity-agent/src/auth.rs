// This file is Copyright its original authors, visible in version control history.
//
// This file is licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. You may not use this file except in
// accordance with one or both of these licenses.

//! Databricks user-to-machine OAuth login (authorization code with PKCE), the way the goose
//! app signs in when personal access tokens are disabled for a workspace.
//!
//! The GDK's Databricks provider asks a token-provider closure for an access token on every
//! request. The closure here keeps the tokens in memory and in a file, refreshes them with the
//! refresh token when they are about to expire, and only opens a browser when it has nothing
//! usable. Endpoints follow the Databricks OAuth docs: `/oidc/v1/authorize` and
//! `/oidc/v1/token` on the workspace host.

use std::io::{Read, Write};
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, bail, Context, Result};
use goose_providers::databricks_auth::DatabricksOauthTokenProvider;
use ldk_node::bitcoin::hashes::{sha256, Hash};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

/// The port goose and the Databricks CLI listen on for the login redirect.
const REDIRECT_PORT: u16 = 8020;
/// Refresh this long before the access token expires.
const REFRESH_MARGIN: Duration = Duration::from_secs(120);
const LOGIN_TIMEOUT: Duration = Duration::from_secs(300);

#[derive(Clone, Serialize, Deserialize)]
struct Tokens {
	access_token: String,
	refresh_token: Option<String>,
	/// Unix seconds.
	expires_at: u64,
}

#[derive(Deserialize)]
struct TokenResponse {
	access_token: String,
	refresh_token: Option<String>,
	expires_in: Option<u64>,
}

fn now_secs() -> u64 {
	SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)
}

fn base64url(bytes: &[u8]) -> String {
	const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
	let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
	for chunk in bytes.chunks(3) {
		let n = chunk.iter().fold(0u32, |acc, &b| (acc << 8) | b as u32) << (8 * (3 - chunk.len()));
		for i in 0..=chunk.len() {
			out.push(ALPHABET[((n >> (18 - 6 * i)) & 63) as usize] as char);
		}
	}
	out
}

fn percent_decode(input: &str) -> String {
	let bytes = input.as_bytes();
	let mut out = Vec::with_capacity(bytes.len());
	let mut i = 0;
	while i < bytes.len() {
		match bytes[i] {
			b'%' if i + 2 < bytes.len() => {
				let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).unwrap_or("");
				match u8::from_str_radix(hex, 16) {
					Ok(b) => {
						out.push(b);
						i += 3;
					},
					Err(_) => {
						out.push(b'%');
						i += 1;
					},
				}
			},
			b'+' => {
				out.push(b' ');
				i += 1;
			},
			b => {
				out.push(b);
				i += 1;
			},
		}
	}
	String::from_utf8_lossy(&out).into_owned()
}

/// Where the tokens live: `DATABRICKS_TOKEN_FILE`, else
/// `~/.ldk-liquidity-agent/databricks_oauth.json`. Not under the node's storage directory,
/// which demo mode wipes on every run.
pub fn default_token_path() -> Result<PathBuf> {
	if let Some(path) = std::env::var_os("DATABRICKS_TOKEN_FILE").filter(|p| !p.is_empty()) {
		return Ok(PathBuf::from(path));
	}
	let home = std::env::var_os("HOME").context("HOME is not set; set DATABRICKS_TOKEN_FILE")?;
	Ok(PathBuf::from(home).join(".ldk-liquidity-agent").join("databricks_oauth.json"))
}

pub struct Login {
	path: PathBuf,
	http: reqwest::Client,
	tokens: Mutex<Option<Tokens>>,
}

/// The OAuth client goose and the Databricks CLI use, which every workspace knows.
const CLIENT_ID: &str = "databricks-cli";
const REDIRECT_URL: &str = "http://localhost";
const SCOPES: [&str; 2] = ["all-apis", "offline_access"];

impl Login {
	pub fn new(path: PathBuf) -> Self {
		Self { path, http: reqwest::Client::new(), tokens: Mutex::new(None) }
	}

	/// Signs in now if the cached tokens are missing or stale, so the browser step happens
	/// up front and never inside a model request, which the provider may time out and retry
	/// while the redirect listener is still waiting.
	pub async fn ensure_signed_in(&self, host: &str) -> Result<()> {
		let scopes = SCOPES.iter().map(|s| s.to_string()).collect();
		self.access_token(host.to_string(), CLIENT_ID.to_string(), REDIRECT_URL.to_string(), scopes)
			.await
			.map(|_| ())
	}

	fn load(&self) -> Option<Tokens> {
		let bytes = std::fs::read(&self.path).ok()?;
		serde_json::from_slice(&bytes).ok()
	}

	fn save(&self, tokens: &Tokens) -> Result<()> {
		if let Some(parent) = self.path.parent() {
			std::fs::create_dir_all(parent)?;
		}
		let mut options = std::fs::OpenOptions::new();
		options.write(true).create(true).truncate(true);
		#[cfg(unix)]
		{
			use std::os::unix::fs::OpenOptionsExt;
			options.mode(0o600);
		}
		let mut file =
			options.open(&self.path).with_context(|| format!("write {}", self.path.display()))?;
		file.write_all(&serde_json::to_vec_pretty(tokens)?)?;
		Ok(())
	}

	async fn access_token(
		&self, host: String, client_id: String, redirect_url: String, scopes: Vec<String>,
	) -> Result<String> {
		let host = normalize_host(&host);
		let mut guard = self.tokens.lock().await;
		if guard.is_none() {
			*guard = self.load();
		}
		if let Some(tokens) = guard.as_ref() {
			if tokens.expires_at > now_secs() + REFRESH_MARGIN.as_secs() {
				return Ok(tokens.access_token.clone());
			}
			if let Some(refresh_token) = tokens.refresh_token.clone() {
				match self.refresh(&host, &client_id, &refresh_token).await {
					Ok(fresh) => {
						let token = fresh.access_token.clone();
						self.save(&fresh)?;
						*guard = Some(fresh);
						return Ok(token);
					},
					Err(e) => eprintln!("[auth] refresh failed ({e}); logging in again"),
				}
			}
		}
		let fresh = self.login(&host, &client_id, &redirect_url, &scopes).await?;
		let token = fresh.access_token.clone();
		self.save(&fresh)?;
		eprintln!("[auth] logged in to {host}; tokens saved to {}", self.path.display());
		*guard = Some(fresh);
		Ok(token)
	}

	async fn login(
		&self, host: &str, client_id: &str, redirect_url: &str, scopes: &[String],
	) -> Result<Tokens> {
		// 48 random bytes -> 64 URL-safe characters, inside PKCE's 43..128 range.
		let mut random = Vec::with_capacity(48);
		for _ in 0..3 {
			random.extend_from_slice(uuid::Uuid::new_v4().as_bytes());
		}
		let verifier = base64url(&random);
		let challenge = base64url(sha256::Hash::hash(verifier.as_bytes()).as_byte_array());
		let state = uuid::Uuid::new_v4().to_string();
		let redirect_uri = format!("{redirect_url}:{REDIRECT_PORT}");

		// Bind before opening the browser so the redirect cannot arrive to a closed port.
		let listener = TcpListener::bind(("127.0.0.1", REDIRECT_PORT)).with_context(|| {
			format!("listen on 127.0.0.1:{REDIRECT_PORT} for the login redirect")
		})?;
		listener.set_nonblocking(false)?;

		let url = reqwest::Url::parse_with_params(
			&format!("{host}/oidc/v1/authorize"),
			&[
				("response_type", "code"),
				("client_id", client_id),
				("redirect_uri", &redirect_uri),
				("scope", &scopes.join(" ")),
				("state", &state),
				("code_challenge", &challenge),
				("code_challenge_method", "S256"),
			],
		)?;
		eprintln!("[auth] opening the browser to sign in to Databricks; if it does not open, visit:\n{url}");
		open_browser(url.as_str());

		let expected_state = state.clone();
		let code = tokio::time::timeout(
			LOGIN_TIMEOUT,
			tokio::task::spawn_blocking(move || wait_for_redirect(listener, &expected_state)),
		)
		.await
		.map_err(|_| anyhow!("no login redirect within {}s", LOGIN_TIMEOUT.as_secs()))???;

		self.token_request(
			host,
			&[
				("grant_type", "authorization_code"),
				("code", &code),
				("redirect_uri", &redirect_uri),
				("client_id", client_id),
				("code_verifier", &verifier),
			],
		)
		.await
	}

	async fn refresh(&self, host: &str, client_id: &str, refresh_token: &str) -> Result<Tokens> {
		let mut tokens = self
			.token_request(
				host,
				&[
					("grant_type", "refresh_token"),
					("refresh_token", refresh_token),
					("client_id", client_id),
				],
			)
			.await?;
		// Keep the old refresh token if the server did not rotate it.
		if tokens.refresh_token.is_none() {
			tokens.refresh_token = Some(refresh_token.to_owned());
		}
		Ok(tokens)
	}

	async fn token_request(&self, host: &str, form: &[(&str, &str)]) -> Result<Tokens> {
		let response = self
			.http
			.post(format!("{host}/oidc/v1/token"))
			.form(form)
			.send()
			.await
			.context("token request")?;
		let status = response.status();
		let body = response.text().await.unwrap_or_default();
		if !status.is_success() {
			bail!("token endpoint returned {status}: {}", body.trim());
		}
		let parsed: TokenResponse =
			serde_json::from_str(&body).context("token endpoint returned unexpected JSON")?;
		Ok(Tokens {
			access_token: parsed.access_token,
			refresh_token: parsed.refresh_token,
			expires_at: now_secs() + parsed.expires_in.unwrap_or(3600),
		})
	}
}

fn normalize_host(host: &str) -> String {
	let host = host.trim().trim_end_matches('/');
	if host.starts_with("http://") || host.starts_with("https://") {
		host.to_owned()
	} else {
		format!("https://{host}")
	}
}

fn open_browser(url: &str) {
	#[cfg(target_os = "macos")]
	let opener = "open";
	#[cfg(not(target_os = "macos"))]
	let opener = "xdg-open";
	if let Err(e) = Command::new(opener).arg(url).spawn() {
		eprintln!("[auth] could not launch {opener} ({e}); open the URL above by hand");
	}
}

/// Accepts the browser's redirect, checks the state, answers with a small page, and returns
/// the authorization code. Blocking; run it off the async runtime.
fn wait_for_redirect(listener: TcpListener, expected_state: &str) -> Result<String> {
	loop {
		let (mut stream, _) = listener.accept().context("accept login redirect")?;
		let mut buf = vec![0u8; 8192];
		let n = stream.read(&mut buf).unwrap_or(0);
		let request = String::from_utf8_lossy(&buf[..n]);
		let Some(target) = request.split_whitespace().nth(1) else { continue };
		let query = target.split_once('?').map(|(_, q)| q).unwrap_or("");
		let mut code = None;
		let mut state = None;
		let mut error = None;
		for pair in query.split('&') {
			let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
			match key {
				"code" => code = Some(percent_decode(value)),
				"state" => state = Some(percent_decode(value)),
				"error" => error = Some(percent_decode(value)),
				_ => {},
			}
		}
		let outcome = match (code, state, error) {
			(_, _, Some(error)) => Err(anyhow!("login was refused: {error}")),
			(Some(code), Some(state), None) if state == expected_state => Ok(code),
			(Some(_), _, None) => Err(anyhow!("login redirect carried an unexpected state")),
			_ => {
				// Not the redirect (a favicon request, say); answer and keep waiting.
				let _ = stream.write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n");
				continue;
			},
		};
		let page = match &outcome {
			Ok(_) => "<html><body><p>Signed in. You can close this tab.</p></body></html>",
			Err(_) => "<html><body><p>Sign-in failed. Check the agent's output.</p></body></html>",
		};
		let _ = stream.write_all(
			format!(
				"HTTP/1.1 200 OK\r\nContent-Type: text/html\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{page}",
				page.len()
			)
			.as_bytes(),
		);
		return outcome;
	}
}

/// The closure the GDK's Databricks provider calls for a bearer token.
pub fn token_provider(login: Arc<Login>) -> DatabricksOauthTokenProvider {
	Arc::new(move |host, client_id, redirect_url, scopes| {
		let login = Arc::clone(&login);
		Box::pin(async move { login.access_token(host, client_id, redirect_url, scopes).await })
	})
}
