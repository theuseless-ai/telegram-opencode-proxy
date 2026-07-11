//! Hermetic end-to-end tests of the admin control socket (issues #38 / #39).
//!
//! `status` (#38): binds a real [`serve_admin`] on a tempdir Unix socket whose
//! one slot points at an in-process [`MockOpencode`], drives the real
//! [`send_request`] client, and asserts the live readiness probe reports the
//! slot **connected** — exercising the whole CLI ↔ daemon channel with no
//! network and no real opencode.
//!
//! `connect` (#39): drives `serve_admin` + `send_request` against a real
//! [`AppState`] (registry + in-memory `Db`) pointed at a `MockOpencode`, and
//! asserts the three idempotent outcomes — `added` (+ persisted), `connected`,
//! `reconnected` — plus the clear error when a down slot stays unreachable.
//!
//! `connected == false` (down slots), the `0600` permission enforcement, and the
//! stale-socket replacement are covered by the unit tests in `src/admin.rs`.

// Pull in only the opencode mock (not mock_telegram) so this crate has no unused
// support code; `#[allow(dead_code)]` covers the mock's unexercised helpers.
#[path = "support/mock_opencode.rs"]
#[allow(dead_code)]
mod mock_opencode;

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use telegram_opencode_proxy::admin::{
    self, AdminRequest, AdminResponse, AdminState, BoxFuture, ConnectOutcome, ConnectParams,
    SlotInfo,
};
use telegram_opencode_proxy::config::{Config, Model, Permissions, Slot};
use telegram_opencode_proxy::opencode::client::OpencodeClient;
use telegram_opencode_proxy::persistence::Db;
use telegram_opencode_proxy::state::SlotConn;
use telegram_opencode_proxy::telegram::bot::AppState;

use mock_opencode::MockOpencode;

/// A minimal [`AdminState`] with a fixed slot list — stands in for `AppState`
/// in the `status`-only transport test.
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
    ) -> BoxFuture<'a, anyhow::Result<ConnectOutcome>> {
        Box::pin(async { anyhow::bail!("FakeState does not support connect") })
    }
}

/// Retry `send_request` briefly so a test doesn't race the server bind.
async fn send_retry(socket: &Path, req: &AdminRequest) -> AdminResponse {
    for _ in 0..50 {
        match admin::send_request(socket, req).await {
            Ok(resp) => return resp,
            Err(_) => tokio::time::sleep(Duration::from_millis(10)).await,
        }
    }
    panic!("admin socket never became reachable");
}

/// A config carrying only the model selector the `MockOpencode` catalogue
/// advertises; slots come from the runtime registry, not config, in these tests.
fn cfg_with_model() -> Config {
    Config {
        bot_token: "t".to_string(),
        admin_socket: "/tmp/unused.sock".into(),
        slots: Vec::new(),
        model: Model {
            provider_id: "llm-lan".to_string(),
            model_id: "Qwen3.6-35B-A3B-bf16".to_string(),
        },
        permissions: Permissions { ask: Vec::new() },
        db_path: "unused.db".into(),
    }
}

/// A live [`SlotConn`] pointing at `url` (client built directly, no bring-up).
fn slot_conn(name: &str, url: &str, telegram_id: Option<i64>) -> SlotConn {
    SlotConn {
        slot: Slot {
            name: name.to_string(),
            opencode_url: url.to_string(),
            workdir: ".".into(),
            telegram_id,
        },
        client: OpencodeClient::new(url).expect("opencode client builds"),
    }
}

#[tokio::test]
async fn status_reports_a_live_slot_as_connected() {
    let oc = MockOpencode::start().await;
    let dir = tempfile::tempdir().expect("tempdir");
    let socket = dir.path().join("admin.sock");

    let state: Arc<dyn AdminState> = Arc::new(FakeState {
        slots: vec![SlotInfo {
            name: "you".to_string(),
            opencode_url: oc.url.clone(),
        }],
    });
    let server = tokio::spawn(admin::serve_admin(Arc::clone(&state), socket.clone()));

    match send_retry(&socket, &AdminRequest::Status).await {
        AdminResponse::Status { slots } => {
            assert_eq!(slots.len(), 1);
            assert_eq!(slots[0].name, "you");
            assert_eq!(slots[0].opencode_url, oc.url);
            assert!(
                slots[0].connected,
                "a slot pointing at a live opencode must report connected"
            );
        }
        other => panic!("expected Status, got {other:?}"),
    }

    server.abort();
}

/// `connect` a slot that does not exist → `added`, live in the registry, and
/// persisted to the `slots` table so it survives a restart.
#[tokio::test]
async fn connect_adds_and_persists_a_new_slot() {
    let oc = MockOpencode::start().await;
    let dir = tempfile::tempdir().expect("tempdir");
    let socket = dir.path().join("admin.sock");
    let db = Db::open_in_memory().expect("in-memory db");
    let state = AppState::new(cfg_with_model(), HashMap::new(), db.clone());
    let server = tokio::spawn(admin::serve_admin(state.clone(), socket.clone()));

    let req = AdminRequest::Connect {
        name: "new".to_string(),
        url: Some(oc.url.clone()),
        workdir: Some(".".to_string()),
        telegram_id: Some(555),
    };
    match send_retry(&socket, &req).await {
        AdminResponse::Connect { name, outcome } => {
            assert_eq!(name, "new");
            assert_eq!(outcome, ConnectOutcome::Added);
        }
        other => panic!("expected Connect, got {other:?}"),
    }

    // The live registry now carries it...
    assert!(
        state.registry.read().unwrap().contains_key("new"),
        "added slot must be live in the registry"
    );
    // ...and it is persisted (survives a restart).
    let persisted = db.list_slots().unwrap();
    assert_eq!(persisted.len(), 1, "added slot must be persisted");
    assert_eq!(persisted[0].name, "new");
    assert_eq!(persisted[0].opencode_url, oc.url);
    assert_eq!(persisted[0].telegram_id, Some(555));

    server.abort();
}

/// `connect` an existing slot whose opencode is up → `connected` (no-op).
#[tokio::test]
async fn connect_reports_existing_reachable_slot_as_connected() {
    let oc = MockOpencode::start().await;
    let dir = tempfile::tempdir().expect("tempdir");
    let socket = dir.path().join("admin.sock");
    let mut registry = HashMap::new();
    registry.insert("you".to_string(), slot_conn("you", &oc.url, Some(111)));
    let state = AppState::new(cfg_with_model(), registry, Db::open_in_memory().unwrap());
    let server = tokio::spawn(admin::serve_admin(state.clone(), socket.clone()));

    let req = AdminRequest::Connect {
        name: "you".to_string(),
        url: None,
        workdir: None,
        telegram_id: None,
    };
    match send_retry(&socket, &req).await {
        AdminResponse::Connect { name, outcome } => {
            assert_eq!(name, "you");
            assert_eq!(outcome, ConnectOutcome::Connected);
        }
        other => panic!("expected Connect, got {other:?}"),
    }

    server.abort();
}

/// `connect` an existing slot that is down at probe time but reachable again for
/// the reconnect bring-up → `reconnected`.
#[tokio::test]
async fn connect_reconnects_a_down_slot_when_it_returns() {
    // First `GET /config` (the probe) sees 503; the reconnect bring-up sees 200.
    let oc = MockOpencode::start_with_config_failures(1).await;
    let dir = tempfile::tempdir().expect("tempdir");
    let socket = dir.path().join("admin.sock");
    let mut registry = HashMap::new();
    registry.insert("you".to_string(), slot_conn("you", &oc.url, Some(111)));
    let state = AppState::new(cfg_with_model(), registry, Db::open_in_memory().unwrap());
    let server = tokio::spawn(admin::serve_admin(state.clone(), socket.clone()));

    let req = AdminRequest::Connect {
        name: "you".to_string(),
        url: None,
        workdir: None,
        telegram_id: None,
    };
    match send_retry(&socket, &req).await {
        AdminResponse::Connect { outcome, .. } => {
            assert_eq!(outcome, ConnectOutcome::Reconnected);
        }
        other => panic!("expected Connect, got {other:?}"),
    }

    server.abort();
}

/// `connect` an existing slot whose opencode never comes back → a clear error.
#[tokio::test]
async fn connect_errors_when_a_down_slot_stays_unreachable() {
    let dir = tempfile::tempdir().expect("tempdir");
    let socket = dir.path().join("admin.sock");
    let dead = "http://127.0.0.1:1"; // port 1 is not listening
    let mut registry = HashMap::new();
    registry.insert("you".to_string(), slot_conn("you", dead, Some(111)));
    let state = AppState::new(cfg_with_model(), registry, Db::open_in_memory().unwrap());
    let server = tokio::spawn(admin::serve_admin(state.clone(), socket.clone()));

    let req = AdminRequest::Connect {
        name: "you".to_string(),
        url: None,
        workdir: None,
        telegram_id: None,
    };
    match send_retry(&socket, &req).await {
        AdminResponse::Error { message } => {
            assert!(
                message.contains("not reachable") || message.contains("reconnecting"),
                "expected a clear reachability error, got: {message}"
            );
        }
        other => panic!("expected Error, got {other:?}"),
    }

    server.abort();
}
