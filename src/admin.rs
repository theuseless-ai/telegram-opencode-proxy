//! Admin control channel: a **local-only** Unix-domain socket that the CLI
//! (`proxy status`, later `proxy connect` #39 and `proxy pair …` #4b) uses to
//! talk to the running daemon. See `docs/design/architecture.md` §5.
//!
//! # Security boundary (hold this line)
//!
//! opencode executes code, so this channel is privileged. It is **never** put on
//! the network: it is a filesystem Unix socket, and [`serve_admin`] `chmod`s it
//! to `0600` and **verifies** the mode before serving — refusing to run if the
//! kernel didn't honour the permissions. Any stale socket left by a previous
//! process is unlinked before bind, and the socket is removed on shutdown.
//!
//! # Wire protocol
//!
//! Newline-delimited JSON, one request line in, one response line out, then the
//! connection closes. Requests/responses are internally tagged enums
//! ([`AdminRequest`] / [`AdminResponse`]) so new commands are a variant plus a
//! match arm — #39 adds `Connect`, #4b adds `Pair*` — without breaking the frame
//! format or the existing `status` client.

use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};

use crate::opencode::health;

/// Required socket permissions: owner read/write only. This is the security
/// boundary — [`serve_admin`] sets **and verifies** this mode.
const SOCKET_MODE: u32 = 0o600;

/// Connect timeout for a slot readiness probe. Short so `status` never hangs on
/// a wedged host.
const PROBE_CONNECT_TIMEOUT: Duration = Duration::from_millis(500);

/// Overall timeout for a slot readiness probe request.
const PROBE_TIMEOUT: Duration = Duration::from_secs(2);

/// Inter-attempt sleep for the single-shot readiness probe. Tiny — with one
/// attempt it only ever runs once on failure (see [`probe_ready`]).
const PROBE_INTERVAL: Duration = Duration::from_millis(1);

/// A request from an admin CLI to the daemon.
///
/// `#[serde(tag = "cmd")]` keeps the wire form self-describing and extensible:
/// #39 will add `Connect { slot }`, #4b `PairList` / `PairApprove { code, slot }`
/// / `PairDeny { code }`. Each is a new variant here plus an arm in [`handle`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "cmd", rename_all = "snake_case")]
pub enum AdminRequest {
    /// Report every configured slot and whether its opencode is reachable now.
    Status,
    /// Idempotently ensure `name` is connected (#39): report `connected` if it
    /// is already up, `reconnect` it if it exists but its opencode is down, or
    /// `add` it (requires `url`) if it does not exist. An added slot is also
    /// persisted so it survives a restart.
    Connect {
        /// Slot name to ensure connected.
        name: String,
        /// opencode base URL — **required** when adding a slot that does not
        /// exist; ignored for a slot that already exists.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        url: Option<String>,
        /// Working directory for a newly-added slot (defaults to `.`).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        workdir: Option<String>,
        /// Telegram id to bind a newly-added slot to.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        telegram_id: Option<i64>,
    },
}

/// A response from the daemon to an admin CLI.
///
/// `#[serde(tag = "resp")]` mirrors [`AdminRequest`]: future commands add reply
/// variants without disturbing existing ones. Any handler failure is reported as
/// [`AdminResponse::Error`] rather than a dropped connection or a panic.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "resp", rename_all = "snake_case")]
pub enum AdminResponse {
    /// Reply to [`AdminRequest::Status`].
    Status {
        /// One entry per configured slot, in config order.
        slots: Vec<SlotStatus>,
    },
    /// Reply to [`AdminRequest::Connect`] — which of the three idempotent
    /// outcomes was taken for `name`.
    Connect {
        /// The slot the command acted on.
        name: String,
        /// What the daemon did: already up, reconnected, or newly added.
        outcome: ConnectOutcome,
    },
    /// A handler failed; `message` is human-readable.
    Error {
        /// What went wrong.
        message: String,
    },
}

/// The idempotent result of an [`AdminRequest::Connect`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConnectOutcome {
    /// The slot existed and its opencode was already reachable — a no-op.
    Connected,
    /// The slot existed but was down; its client was rebuilt and re-validated.
    Reconnected,
    /// The slot did not exist; it was built, connected, and persisted.
    Added,
}

/// Parameters for [`AdminState::ensure_connected`], mirroring
/// [`AdminRequest::Connect`] minus the wire framing.
#[derive(Debug, Clone)]
pub struct ConnectParams {
    /// Slot name to ensure connected.
    pub name: String,
    /// opencode base URL — required only when adding a new slot.
    pub url: Option<String>,
    /// Working directory for a newly-added slot (defaults to `.`).
    pub workdir: Option<String>,
    /// Telegram id to bind a newly-added slot to.
    pub telegram_id: Option<i64>,
}

/// A boxed, `Send` future — the return type for the async [`AdminState`] method,
/// hand-written so the trait stays object-safe (`Arc<dyn AdminState>`) without
/// pulling in the `async-trait` macro.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Per-slot status line in an [`AdminResponse::Status`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SlotStatus {
    /// Slot name (matches `config.toml`).
    pub name: String,
    /// The opencode base URL the slot routes to.
    pub opencode_url: String,
    /// `true` iff a fresh readiness ping to `opencode_url` just succeeded.
    pub connected: bool,
}

/// A configured slot, as far as the admin handlers care: just enough to report
/// and probe it. Kept separate from `config::Slot` so the [`AdminState`] trait
/// stays small and testable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlotInfo {
    /// Slot name.
    pub name: String,
    /// The opencode base URL.
    pub opencode_url: String,
}

/// The read-only slice of daemon state the admin handlers need.
///
/// `AppState` implements this in `telegram::bot`. Keeping handlers behind a
/// trait (rather than a concrete `Arc<AppState>`) means the dispatch logic is
/// unit-testable with a tiny fake, and #39/#4b can widen the trait (runtime
/// slots, `Db` access) without touching the transport layer. `'static` so it can
/// ride an `Arc<dyn AdminState>` across spawned connection tasks.
pub trait AdminState: Send + Sync + 'static {
    /// The live slots to report and probe, in a stable order.
    fn slots(&self) -> Vec<SlotInfo>;

    /// Idempotently ensure `params.name` is connected (#39): report the slot
    /// already up, reconnect it, or add+persist it. Mutates the live registry.
    /// Returns a boxed future so the trait stays object-safe.
    fn ensure_connected<'a>(
        &'a self,
        params: ConnectParams,
    ) -> BoxFuture<'a, Result<ConnectOutcome>>;
}

/// Serve the admin control socket at `socket_path` until the future is dropped
/// (the daemon cancels it on dispatcher shutdown; see `lib::serve`).
///
/// Bring-up, in order: unlink any **stale** socket, bind a [`UnixListener`],
/// `chmod` to `0600` and **verify** the mode (bail otherwise — the socket is a
/// security boundary), then accept forever. Each connection is one request line
/// → one response line → close, handled on its own task so a slow probe cannot
/// block the accept loop. Handler failures become [`AdminResponse::Error`]; a
/// connection never panics the daemon.
pub async fn serve_admin(state: Arc<dyn AdminState>, socket_path: PathBuf) -> Result<()> {
    let socket_path = socket_path.as_path();
    remove_stale(socket_path)?;

    let listener = UnixListener::bind(socket_path)
        .with_context(|| format!("binding admin socket at {}", socket_path.display()))?;

    // Lock the permissions down and prove it before we accept a single byte.
    if let Err(err) = enforce_mode(socket_path) {
        // Don't leave a wrongly-permissioned socket lying around.
        let _ = std::fs::remove_file(socket_path);
        return Err(err);
    }

    let http = probe_client()?;
    tracing::info!(socket = %socket_path.display(), "admin control socket listening (local-only, 0600)");

    loop {
        match listener.accept().await {
            Ok((stream, _addr)) => {
                let state = Arc::clone(&state);
                let http = http.clone();
                tokio::spawn(async move {
                    if let Err(err) = handle_conn(stream, state, http).await {
                        tracing::warn!(error = %err, "admin connection failed");
                    }
                });
            }
            Err(err) => tracing::warn!(error = %err, "admin socket accept failed"),
        }
    }
}

/// Client half: connect to the daemon's admin socket, send one request line,
/// read one response line. Runs in a *separate* process (e.g. `proxy status`)
/// from the daemon that owns the socket.
pub async fn send_request(socket_path: &Path, req: &AdminRequest) -> Result<AdminResponse> {
    let stream = UnixStream::connect(socket_path).await.with_context(|| {
        format!(
            "connecting to admin socket {} — is the daemon running?",
            socket_path.display()
        )
    })?;
    let (read_half, mut write_half) = stream.into_split();

    let mut line = serde_json::to_string(req).context("serializing admin request")?;
    line.push('\n');
    write_half
        .write_all(line.as_bytes())
        .await
        .context("writing admin request")?;
    write_half.flush().await.context("flushing admin request")?;

    let mut reader = BufReader::new(read_half);
    let mut resp_line = String::new();
    reader
        .read_line(&mut resp_line)
        .await
        .context("reading admin response")?;
    ensure!(
        !resp_line.trim().is_empty(),
        "admin socket closed without a response"
    );
    serde_json::from_str(resp_line.trim_end()).context("parsing admin response")
}

/// Handle one connection: read a request line, dispatch, write a response line,
/// close. A malformed request line becomes an [`AdminResponse::Error`] rather
/// than an error return.
async fn handle_conn(
    stream: UnixStream,
    state: Arc<dyn AdminState>,
    http: reqwest::Client,
) -> Result<()> {
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);
    let mut line = String::new();
    let read = reader
        .read_line(&mut line)
        .await
        .context("reading admin request line")?;
    if read == 0 {
        return Ok(()); // client connected but sent nothing; nothing to answer.
    }

    let response = match serde_json::from_str::<AdminRequest>(line.trim_end()) {
        Ok(req) => dispatch(state.as_ref(), &http, req).await,
        Err(err) => AdminResponse::Error {
            message: format!("invalid admin request: {err}"),
        },
    };

    let mut out = serde_json::to_string(&response).context("serializing admin response")?;
    out.push('\n');
    write_half
        .write_all(out.as_bytes())
        .await
        .context("writing admin response")?;
    write_half
        .flush()
        .await
        .context("flushing admin response")?;
    let _ = write_half.shutdown().await;
    Ok(())
}

/// Route a request to its handler, mapping any handler error onto
/// [`AdminResponse::Error`]. New commands slot into [`handle`] and inherit this
/// uniform failure mapping.
async fn dispatch(
    state: &dyn AdminState,
    http: &reqwest::Client,
    req: AdminRequest,
) -> AdminResponse {
    match handle(state, http, req).await {
        Ok(resp) => resp,
        Err(err) => AdminResponse::Error {
            message: format!("{err:#}"),
        },
    }
}

/// The fallible handler core. `Status` cannot fail today, but returning `Result`
/// gives #39/#4b handlers a `?`-friendly seam that already funnels into
/// [`AdminResponse::Error`] via [`dispatch`].
async fn handle(
    state: &dyn AdminState,
    http: &reqwest::Client,
    req: AdminRequest,
) -> Result<AdminResponse> {
    Ok(match req {
        AdminRequest::Status => AdminResponse::Status {
            slots: status(state, http).await,
        },
        AdminRequest::Connect {
            name,
            url,
            workdir,
            telegram_id,
        } => {
            let outcome = state
                .ensure_connected(ConnectParams {
                    name: name.clone(),
                    url,
                    workdir,
                    telegram_id,
                })
                .await?;
            AdminResponse::Connect { name, outcome }
        }
    })
}

/// Assemble the `Status` reply: for each configured slot, a fresh readiness ping.
async fn status(state: &dyn AdminState, http: &reqwest::Client) -> Vec<SlotStatus> {
    let mut out = Vec::new();
    for slot in state.slots() {
        let connected = probe_ready(http, &slot.opencode_url).await;
        out.push(SlotStatus {
            name: slot.name,
            opencode_url: slot.opencode_url,
            connected,
        });
    }
    out
}

/// A single, short readiness ping to a slot's opencode — one `wait_ready`
/// attempt, so `status` reports the live truth without blocking for the full
/// 60 s startup budget.
async fn probe_ready(http: &reqwest::Client, base_url: &str) -> bool {
    health::wait_ready(http, base_url, 1, PROBE_INTERVAL)
        .await
        .is_ok()
}

/// Build the HTTP client used for readiness probes, with short timeouts so a
/// down or wedged slot fails fast.
fn probe_client() -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .connect_timeout(PROBE_CONNECT_TIMEOUT)
        .timeout(PROBE_TIMEOUT)
        .build()
        .context("building admin readiness-probe HTTP client")
}

/// Unlink a stale socket file left by a previous process. Missing is fine; any
/// other error (e.g. a directory in the way) is surfaced.
fn remove_stale(path: &Path) -> Result<()> {
    match std::fs::remove_file(path) {
        Ok(()) => {
            tracing::debug!(socket = %path.display(), "removed stale admin socket");
            Ok(())
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => {
            Err(err).with_context(|| format!("removing stale admin socket {}", path.display()))
        }
    }
}

/// `chmod` the socket to `0600` and verify the kernel applied it. The verify is
/// the point: this is a privileged channel, so we refuse to serve on anything
/// looser than owner-only.
fn enforce_mode(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    std::fs::set_permissions(path, std::fs::Permissions::from_mode(SOCKET_MODE))
        .with_context(|| format!("chmod 0600 on admin socket {}", path.display()))?;

    let mode = std::fs::metadata(path)
        .with_context(|| format!("stat admin socket {}", path.display()))?
        .permissions()
        .mode()
        & 0o777;
    ensure!(
        mode == SOCKET_MODE,
        "admin socket {} has mode {mode:04o}, expected 0600 — refusing to expose the control channel",
        path.display()
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::os::unix::fs::PermissionsExt;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    /// A tiny [`AdminState`] fake for the pure-handler and transport tests.
    struct FakeState {
        slots: Vec<SlotInfo>,
    }

    impl AdminState for FakeState {
        fn slots(&self) -> Vec<SlotInfo> {
            self.slots.clone()
        }

        fn ensure_connected<'a>(
            &'a self,
            _params: ConnectParams,
        ) -> BoxFuture<'a, Result<ConnectOutcome>> {
            Box::pin(async { anyhow::bail!("FakeState does not support connect") })
        }
    }

    fn fake(slots: &[(&str, &str)]) -> Arc<dyn AdminState> {
        Arc::new(FakeState {
            slots: slots
                .iter()
                .map(|(n, u)| SlotInfo {
                    name: (*n).to_string(),
                    opencode_url: (*u).to_string(),
                })
                .collect(),
        })
    }

    /// A base URL that is guaranteed unreachable (port 1, not listening).
    const DEAD_URL: &str = "http://127.0.0.1:1";

    /// Spawn a throwaway HTTP responder that answers any request with `200 {}` —
    /// enough to satisfy `wait_ready`'s `GET /config` probe. Returns its base URL.
    async fn spawn_ok_health() -> String {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind health responder");
        let addr = listener.local_addr().expect("health responder addr");
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let mut buf = [0u8; 1024];
                    let _ = stream.read(&mut buf).await; // drain the request line(s)
                    let _ = stream
                        .write_all(
                            b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}",
                        )
                        .await;
                    let _ = stream.shutdown().await;
                });
            }
        });
        format!("http://{addr}")
    }

    /// Retry `send_request` briefly so the test doesn't race the server's bind.
    async fn status_request(socket: &Path) -> AdminResponse {
        for _ in 0..50 {
            match send_request(socket, &AdminRequest::Status).await {
                Ok(resp) => return resp,
                Err(_) => tokio::time::sleep(Duration::from_millis(10)).await,
            }
        }
        panic!("admin socket never became reachable");
    }

    #[test]
    fn request_serializes_to_tagged_json() {
        let json = serde_json::to_string(&AdminRequest::Status).unwrap();
        assert_eq!(json, r#"{"cmd":"status"}"#);
    }

    #[test]
    fn connect_request_round_trips_and_omits_absent_options() {
        // A bare connect (existing slot) carries no optional fields on the wire.
        let bare = AdminRequest::Connect {
            name: "you".into(),
            url: None,
            workdir: None,
            telegram_id: None,
        };
        assert_eq!(
            serde_json::to_string(&bare).unwrap(),
            r#"{"cmd":"connect","name":"you"}"#
        );

        let full = AdminRequest::Connect {
            name: "new".into(),
            url: Some("http://127.0.0.1:4099".into()),
            workdir: Some("/srv/new".into()),
            telegram_id: Some(42),
        };
        let back: AdminRequest =
            serde_json::from_str(&serde_json::to_string(&full).unwrap()).unwrap();
        assert_eq!(full, back);

        let resp = AdminResponse::Connect {
            name: "new".into(),
            outcome: ConnectOutcome::Added,
        };
        let back: AdminResponse =
            serde_json::from_str(&serde_json::to_string(&resp).unwrap()).unwrap();
        assert_eq!(resp, back);
    }

    #[test]
    fn response_round_trips_through_json() {
        let resp = AdminResponse::Status {
            slots: vec![SlotStatus {
                name: "you".into(),
                opencode_url: "http://127.0.0.1:4096".into(),
                connected: true,
            }],
        };
        let line = serde_json::to_string(&resp).unwrap();
        let back: AdminResponse = serde_json::from_str(&line).unwrap();
        assert_eq!(resp, back);

        let err = AdminResponse::Error {
            message: "boom".into(),
        };
        let back: AdminResponse =
            serde_json::from_str(&serde_json::to_string(&err).unwrap()).unwrap();
        assert_eq!(err, back);
    }

    #[tokio::test]
    async fn status_reports_down_for_unreachable_slot() {
        // Pure-handler test: no socket, just the dispatch core against a fake.
        let http = probe_client().unwrap();
        let state = fake(&[("you", DEAD_URL)]);
        let resp = dispatch(state.as_ref(), &http, AdminRequest::Status).await;
        match resp {
            AdminResponse::Status { slots } => {
                assert_eq!(slots.len(), 1);
                assert_eq!(slots[0].name, "you");
                assert!(!slots[0].connected, "dead port must report disconnected");
            }
            other => panic!("expected Status, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn status_reports_up_for_reachable_slot() {
        let url = spawn_ok_health().await;
        let http = probe_client().unwrap();
        let state = fake(&[("you", &url)]);
        let resp = dispatch(state.as_ref(), &http, AdminRequest::Status).await;
        match resp {
            AdminResponse::Status { slots } => {
                assert_eq!(slots.len(), 1);
                assert!(slots[0].connected, "reachable slot must report connected");
            }
            other => panic!("expected Status, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn round_trips_a_request_over_the_socket() {
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("admin.sock");
        let state = fake(&[("you", DEAD_URL), ("wife", DEAD_URL)]);
        let server = tokio::spawn(serve_admin(Arc::clone(&state), socket.clone()));

        let resp = status_request(&socket).await;
        match resp {
            AdminResponse::Status { slots } => {
                assert_eq!(slots.len(), 2);
                assert_eq!(slots[0].name, "you");
                assert_eq!(slots[1].name, "wife");
                assert!(slots.iter().all(|s| !s.connected));
            }
            other => panic!("expected Status, got {other:?}"),
        }
        server.abort();
    }

    #[tokio::test]
    async fn socket_is_created_with_0600_permissions() {
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("admin.sock");
        let state = fake(&[]);
        let server = tokio::spawn(serve_admin(Arc::clone(&state), socket.clone()));

        // Ensure the socket is up (a successful request implies bind+chmod done).
        let _ = status_request(&socket).await;

        let mode = std::fs::metadata(&socket).unwrap().permissions().mode() & 0o777;
        assert_eq!(
            mode, 0o600,
            "admin socket must be owner-only, got {mode:04o}"
        );
        server.abort();
    }

    #[tokio::test]
    async fn stale_socket_file_is_replaced_cleanly() {
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("admin.sock");
        // Leave a stale *regular file* at the path (as a crashed daemon might).
        std::fs::write(&socket, b"stale").unwrap();
        assert!(socket.exists());

        let state = fake(&[("you", DEAD_URL)]);
        let server = tokio::spawn(serve_admin(Arc::clone(&state), socket.clone()));

        // If bind succeeds, the stale file was unlinked first.
        let resp = status_request(&socket).await;
        assert!(matches!(resp, AdminResponse::Status { .. }));
        let mode = std::fs::metadata(&socket).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
        server.abort();
    }

    #[tokio::test]
    async fn malformed_request_line_yields_error_response() {
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("admin.sock");
        let state = fake(&[]);
        let server = tokio::spawn(serve_admin(Arc::clone(&state), socket.clone()));

        // Wait for the socket, then hand-write a garbage line.
        let _ = status_request(&socket).await;
        let mut stream = UnixStream::connect(&socket).await.unwrap();
        stream.write_all(b"not json at all\n").await.unwrap();
        stream.flush().await.unwrap();
        let mut reader = BufReader::new(stream);
        let mut line = String::new();
        reader.read_line(&mut line).await.unwrap();
        let resp: AdminResponse = serde_json::from_str(line.trim_end()).unwrap();
        match resp {
            AdminResponse::Error { message } => {
                assert!(message.contains("invalid admin request"), "{message}");
            }
            other => panic!("expected Error, got {other:?}"),
        }
        server.abort();
    }
}
