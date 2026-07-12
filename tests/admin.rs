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
use telegram_opencode_proxy::config::{Config, Model, Pairing, Permissions, Slot};
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
        bot_token: "t".into(),
        admin_socket: "/tmp/unused.sock".into(),
        slots: Vec::new(),
        model: Model {
            provider_id: "llm-lan".to_string(),
            model_id: "Qwen3.6-35B-A3B-bf16".to_string(),
        },
        permissions: Permissions { ask: Vec::new() },
        pairing: Pairing::default(),
        db_path: "unused.db".into(),
    }
}

/// A bare bot handle for `AppState` construction; the transport tests here do
/// not drive the pairing notify path.
fn test_bot() -> teloxide::Bot {
    teloxide::Bot::new("12345:test-token")
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
/// persisted to `config.toml` (format-preserving; #45) so it survives a restart.
#[tokio::test]
async fn connect_adds_and_persists_a_new_slot() {
    let oc = MockOpencode::start().await;
    let dir = tempfile::tempdir().expect("tempdir");
    let socket = dir.path().join("admin.sock");

    // A real config file with a comment we expect to survive the write.
    let config_path = dir.path().join("config.toml");
    let original = "\
# keep-me: this comment must survive proxy connect
bot_token = \"t\"
admin_socket = \"/tmp/unused.sock\"

[[slots]]
name = \"you\"
opencode_url = \"http://127.0.0.1:4096\"
workdir = \".\"

[model]
provider_id = \"llm-lan\"
model_id = \"Qwen3.6-35B-A3B-bf16\"
";
    std::fs::write(&config_path, original).expect("write temp config");

    let db = Db::open_in_memory().expect("in-memory db");
    let state = AppState::new(
        cfg_with_model(),
        config_path.clone(),
        HashMap::new(),
        db.clone(),
        test_bot(),
    );
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
    // ...and it is persisted to config.toml (survives a restart), comment intact.
    let written = std::fs::read_to_string(&config_path).expect("read back config");
    assert!(
        written.contains("# keep-me: this comment must survive proxy connect"),
        "the config write must preserve comments, got:\n{written}"
    );
    // Re-parse via the real loader — proves the written file is valid config.
    let cfg = Config::load(&config_path).expect("written config re-loads and validates");
    let added = cfg
        .slots
        .iter()
        .find(|s| s.name == "new")
        .expect("the new slot must be in config.toml");
    assert_eq!(added.opencode_url, oc.url);
    assert_eq!(added.workdir, std::path::PathBuf::from("."));
    assert_eq!(added.telegram_id, Some(555));
    // The pre-existing slot is still there (append, not clobber).
    assert!(cfg.slots.iter().any(|s| s.name == "you"));
    // ...and the --telegram-id is whitelisted immediately (auth reads
    // allowed_users, not the slot's telegram_id column).
    assert_eq!(
        db.allowed_slot(555).unwrap(),
        Some("new".to_string()),
        "connect --telegram-id must write allowed_users so auth authorizes the user"
    );

    server.abort();
}

/// Fix for the startup-crash bug: `connect <name>` with NO --url brings up a
/// CONFIG-declared slot that was skipped at startup (its opencode was down at
/// boot), so an unreachable slot recovers without a restart.
#[tokio::test]
async fn connect_brings_up_a_skipped_config_slot_by_name() {
    let oc = MockOpencode::start().await;
    let dir = tempfile::tempdir().expect("tempdir");
    let socket = dir.path().join("admin.sock");
    let config_path = dir.path().join("config.toml"); // not rewritten for a known slot

    // Config declares "wife" (pointing at the live mock) but the registry is
    // EMPTY — as if her opencode was down at startup and the slot was skipped.
    let mut cfg = cfg_with_model();
    cfg.slots = vec![Slot {
        name: "wife".to_string(),
        opencode_url: oc.url.clone(),
        workdir: ".".into(),
        telegram_id: Some(777),
    }];
    let db = Db::open_in_memory().expect("in-memory db");
    let state = AppState::new(cfg, config_path, HashMap::new(), db.clone(), test_bot());
    let server = tokio::spawn(admin::serve_admin(state.clone(), socket.clone()));

    // Bare `connect wife` (no --url) must resolve from config, not error.
    let req = AdminRequest::Connect {
        name: "wife".to_string(),
        url: None,
        workdir: None,
        telegram_id: None,
    };
    match send_retry(&socket, &req).await {
        AdminResponse::Connect { name, outcome } => {
            assert_eq!(name, "wife");
            assert_eq!(outcome, ConnectOutcome::Connected);
        }
        other => panic!("expected Connect, got {other:?}"),
    }
    assert!(
        state.registry.read().unwrap().contains_key("wife"),
        "the skipped config slot is now live in the registry"
    );
    assert_eq!(
        db.allowed_slot(777).unwrap(),
        Some("wife".to_string()),
        "its config telegram_id is whitelisted on connect"
    );

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
    let state = AppState::new(
        cfg_with_model(),
        "unused.toml".into(),
        registry,
        Db::open_in_memory().unwrap(),
        test_bot(),
    );
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
    let state = AppState::new(
        cfg_with_model(),
        "unused.toml".into(),
        registry,
        Db::open_in_memory().unwrap(),
        test_bot(),
    );
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
    let state = AppState::new(
        cfg_with_model(),
        "unused.toml".into(),
        registry,
        Db::open_in_memory().unwrap(),
        test_bot(),
    );
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
