//! Hermetic integration harness (issue #24, Layer 1).
//!
//! Wires the two in-process mocks (`mock_opencode` + `mock_telegram`) to the
//! REAL proxy path and asserts end-to-end behaviour with **no** network, no
//! `opencode`, and no model. Each test:
//!
//! 1. starts a `mock_opencode` and points a [`Slot`] at it;
//! 2. runs the exact `serve` bring-up (`connect_slots`: readiness → provider
//!    catalogue → `validate_model`) to build the per-slot clients;
//! 3. builds a `teloxide::Bot` whose API URL is the `mock_telegram` base
//!    (`Bot::set_api_url`), so every `sendMessage` is recorded by the mock;
//! 4. drives the real [`handle_text`] handler with a constructed `Message`.
//!
//! Direct-handler invocation (rather than spinning the long-poll dispatcher) is
//! deterministic — no polling races — while still exercising the whole turn:
//! auth gate → `session::get_or_create` (→ mock opencode) → blocking `prompt`
//! (→ mock opencode) → `render::split_message` → `bot.send_message` (→ mock
//! telegram).

mod support;

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use serde_json::json;
use teloxide::Bot;
use teloxide::prelude::Requester;
use teloxide::types::{ChatId, Message};

use telegram_opencode_proxy::admin::{self, AdminRequest, AdminResponse, AdminState};
use telegram_opencode_proxy::config::{Config, Model, Pairing, Permissions, Slot};
use telegram_opencode_proxy::opencode::client::OpencodeClient;
use telegram_opencode_proxy::persistence::Db;
use telegram_opencode_proxy::state::SlotConn;
use telegram_opencode_proxy::telegram::bot::{
    AppState, handle_new, handle_stop, handle_text, handle_verbosity,
};
use telegram_opencode_proxy::telegram::render::Verbosity;
use telegram_opencode_proxy::{connect_slots, spawn_slot_bringup};

use support::mock_opencode::MockOpencode;
use support::mock_telegram::MockTelegram;

const SLOT_ID: i64 = 111;

/// Build a single-slot config pointing at `opencode_url`, bound to `SLOT_ID`.
fn config_for(opencode_url: &str) -> Config {
    Config {
        bot_token: "12345:test-token".into(),
        admin_socket: "/tmp/mock-admin.sock".into(),
        slots: vec![Slot {
            name: "you".to_string(),
            opencode_url: opencode_url.to_string(),
            workdir: ".".into(),
            telegram_id: Some(SLOT_ID),
        }],
        model: Model {
            provider_id: "llm-lan".to_string(),
            model_id: "Qwen3.6-35B-A3B-bf16".to_string(),
        },
        permissions: Permissions { ask: Vec::new() },
        pairing: Pairing::default(),
        db_path: "proxy.db".into(),
    }
}

/// A `teloxide::Bot` whose whole API base is the mock — so requests go to
/// `{mock}/bot{token}/{Method}` and never touch `api.telegram.org`.
fn bot_pointed_at(mock: &MockTelegram) -> Bot {
    Bot::new("12345:test-token").set_api_url(
        mock.url
            .parse()
            .expect("mock_telegram url parses as an API base"),
    )
}

/// A minimal private-chat text `Message` from `chat_id`, deserialized from the
/// Bot API wire shape (the same path teloxide uses for real updates).
fn text_message(chat_id: i64, text: &str) -> Message {
    serde_json::from_value(json!({
        "message_id": 1,
        "date": 0,
        "chat": { "id": chat_id, "type": "private", "first_name": "Tester" },
        "from": { "id": chat_id, "is_bot": false, "first_name": "Tester" },
        "text": text
    }))
    .expect("constructing a teloxide Message from wire JSON")
}

/// Bring up the real per-slot registry for `cfg` against the running mock.
async fn state_for(cfg: Config) -> Arc<AppState> {
    let registry = connect_slots(&cfg)
        .await
        .expect("connect_slots (readiness + model validation) succeeds against the mock");
    // In-memory routing store — hermetic, no file touched by the harness.
    let db = Db::open_in_memory().expect("in-memory persistence store opens");
    // A bare bot handle — the notify path (pairing approval) isn't driven here.
    let bot = Bot::new("12345:test-token");
    // No config file is written in these turn tests, so a placeholder path is fine.
    AppState::new(cfg, "unused.toml".into(), registry, db, bot)
}

/// 1. Authorized text → the mock records a `sendMessage` carrying the model reply.
#[tokio::test]
async fn authorized_text_relays_model_reply() {
    let oc = MockOpencode::start().await;
    let tg = MockTelegram::start().await;
    let state = state_for(config_for(&oc.url)).await;
    let bot = bot_pointed_at(&tg);

    handle_text(bot, text_message(SLOT_ID, "hello there"), state)
        .await
        .expect("handle_text succeeds");

    // The reply is produced by the user's serial worker (#9), so await it.
    let got = wait_for(
        || {
            tg.sent_messages()
                .iter()
                .any(|m| m.text == "echo: hello there")
        },
        Duration::from_secs(5),
    )
    .await;
    assert!(
        got,
        "expected the model reply, got {:?}",
        tg.sent_messages()
    );
    let sent = tg.sent_messages();
    let reply = sent
        .iter()
        .find(|m| m.text == "echo: hello there")
        .expect("reply present");
    assert_eq!(reply.chat_id, SLOT_ID);
}

/// 2. Unauthorized sender → a single "Not authorized…" reply, no opencode turn.
#[tokio::test]
async fn unauthorized_sender_is_rejected() {
    let oc = MockOpencode::start().await;
    let tg = MockTelegram::start().await;
    let state = state_for(config_for(&oc.url)).await;
    let bot = bot_pointed_at(&tg);

    let stranger = 999;
    handle_text(bot, text_message(stranger, "let me in"), state)
        .await
        .expect("handle_text succeeds");

    let sent = tg.sent_messages();
    assert_eq!(sent.len(), 1, "one rejection reply expected, got {sent:?}");
    assert_eq!(sent[0].chat_id, stranger);
    assert!(
        sent[0].text.starts_with("Not authorized"),
        "expected an authorization rejection, got: {}",
        sent[0].text
    );
}

/// 3. Long reply → multiple `sendMessage` chunks, each within the 4096 limit.
#[tokio::test]
async fn long_reply_is_split_into_chunks() {
    let oc = MockOpencode::start().await;
    let tg = MockTelegram::start().await;
    // 9000 chars > 2 × 4096 → at least three chunks.
    oc.set_reply("x".repeat(9000));
    let state = state_for(config_for(&oc.url)).await;
    let bot = bot_pointed_at(&tg);

    handle_text(bot, text_message(SLOT_ID, "give me a wall of text"), state)
        .await
        .expect("handle_text succeeds");

    // Await all chunks (sent by the worker's finalize step).
    let total_chars = || {
        tg.sent_messages()
            .iter()
            .map(|m| m.text.chars().count())
            .sum::<usize>()
    };
    let done = wait_for(|| total_chars() == 9000, Duration::from_secs(5)).await;
    assert!(done, "full reply must arrive, got {} chars", total_chars());

    let sent = tg.sent_messages();
    assert!(
        sent.len() > 1,
        "long reply must be chunked, got {} message(s)",
        sent.len()
    );
    for chunk in &sent {
        assert_eq!(chunk.chat_id, SLOT_ID);
        assert!(
            chunk.text.chars().count() <= 4096,
            "chunk exceeds the Telegram limit: {} chars",
            chunk.text.chars().count()
        );
    }
}

/// 4. A slot that fails bring-up (here: model absent from the catalogue) is
///    SKIPPED, not fatal — startup is best-effort so one bad slot never crashes
///    the daemon. (The error message itself is covered by `validate_model`'s
///    unit tests.)
#[tokio::test]
async fn failing_slot_is_skipped_not_fatal_at_startup() {
    let oc = MockOpencode::start_without_model().await;
    let cfg = config_for(&oc.url);

    // Best-effort: startup does NOT error; the bad slot is simply left out.
    let registry = connect_slots(&cfg)
        .await
        .expect("startup tolerates a slot that fails to bring up");
    assert!(
        !registry.contains_key("you"),
        "the mis-provisioned slot must be skipped"
    );
    assert!(
        registry.is_empty(),
        "no other slots configured, so the registry is empty"
    );
}

// --- A4b pairing round-trip (#4b) ---------------------------------------------

/// A single-slot config whose slot is bound to **nobody** (`telegram_id = None`),
/// so any sender is a stranger routed into the pairing handshake.
fn config_unpaired(opencode_url: &str) -> Config {
    let mut cfg = config_for(opencode_url);
    cfg.slots[0].telegram_id = None;
    cfg
}

/// Build a real [`AppState`] whose `bot` is pointed at the mock Telegram, so the
/// pairing approval notify is recorded and assertable.
async fn state_with_bot(cfg: Config, bot: Bot) -> Arc<AppState> {
    let registry = connect_slots(&cfg)
        .await
        .expect("connect_slots succeeds against the mock");
    let db = Db::open_in_memory().expect("in-memory persistence store opens");
    AppState::new(cfg, "unused.toml".into(), registry, db, bot)
}

/// Retry `send_request` briefly so a test doesn't race the admin-socket bind.
async fn admin_send(socket: &Path, req: &AdminRequest) -> AdminResponse {
    for _ in 0..50 {
        match admin::send_request(socket, req).await {
            Ok(resp) => return resp,
            Err(_) => tokio::time::sleep(Duration::from_millis(10)).await,
        }
    }
    panic!("admin socket never became reachable");
}

/// Pull the first run of 6 consecutive ASCII digits out of a message.
fn extract_code(text: &str) -> String {
    let bytes = text.as_bytes();
    for start in 0..bytes.len() {
        if bytes[start..]
            .iter()
            .take(6)
            .filter(|b| b.is_ascii_digit())
            .count()
            == 6
            && start + 6 <= bytes.len()
        {
            // Ensure it's exactly a 6-digit island (not part of a longer number).
            let before_ok = start == 0 || !bytes[start - 1].is_ascii_digit();
            let after_ok = start + 6 == bytes.len() || !bytes[start + 6].is_ascii_digit();
            if before_ok && after_ok {
                return text[start..start + 6].to_string();
            }
        }
    }
    panic!("no 6-digit code found in {text:?}");
}

/// Full A4b flow: an unknown sender is issued a code (pending row created), the
/// admin approves it over the socket (binds `allowed_users`, removes the pending
/// row, notifies the user), and a subsequent message now routes to opencode.
/// Also exercises `proxy slots` reflecting the new binding.
#[tokio::test]
async fn pairing_round_trip_issues_approves_notifies_and_authorizes() {
    let oc = MockOpencode::start().await;
    let tg = MockTelegram::start().await;
    let bot = bot_pointed_at(&tg);
    let state = state_with_bot(config_unpaired(&oc.url), bot.clone()).await;
    let db = state.db.clone();

    let stranger = 555;

    // 1. Unauthorized text → a 6-digit code, and a pending row in the DB.
    handle_text(
        bot.clone(),
        text_message(stranger, "let me in"),
        state.clone(),
    )
    .await
    .expect("handle_text succeeds");
    let sent = tg.sent_messages();
    assert_eq!(sent.len(), 1, "one pairing reply expected, got {sent:?}");
    assert_eq!(sent[0].chat_id, stranger);
    assert!(
        sent[0].text.starts_with("Not authorized"),
        "got: {}",
        sent[0].text
    );
    let code = extract_code(&sent[0].text);
    assert!(
        db.pairing_by_code(&code).unwrap().is_some(),
        "a pending pairing row must exist for the issued code"
    );

    // 2. Admin approves it over the local socket.
    let dir = tempfile::tempdir().expect("tempdir");
    let socket = dir.path().join("admin.sock");
    let admin_state: Arc<dyn AdminState> = state.clone();
    let server = tokio::spawn(admin::serve_admin(admin_state, socket.clone()));

    let req = AdminRequest::PairApprove {
        code: code.clone(),
        slot: "you".to_string(),
    };
    match admin_send(&socket, &req).await {
        AdminResponse::PairApprove { chat_id, slot, .. } => {
            assert_eq!(chat_id, stranger);
            assert_eq!(slot, "you");
        }
        other => panic!("expected PairApprove, got {other:?}"),
    }

    // allowed_users bound + pending row consumed.
    assert_eq!(db.allowed_slot(stranger).unwrap().as_deref(), Some("you"));
    assert!(
        db.pairing_by_code(&code).unwrap().is_none(),
        "approve must consume the pending row"
    );
    // The user was notified via the bot.
    assert!(
        tg.sent_messages()
            .iter()
            .any(|m| m.chat_id == stranger && m.text.contains("Approved")),
        "the approved user must receive a notification"
    );

    // 3. `proxy slots` reflects the new binding.
    match admin_send(&socket, &AdminRequest::Slots).await {
        AdminResponse::Slots { slots } => {
            let you = slots
                .iter()
                .find(|s| s.name == "you")
                .expect("slot 'you' present in inventory");
            assert!(you.telegram_ids.contains(&stranger));
            assert!(you.connected, "the live mock slot must report connected");
        }
        other => panic!("expected Slots, got {other:?}"),
    }
    server.abort();

    // 4. The same sender is now authorized → routes to opencode (via the worker).
    handle_text(
        bot.clone(),
        text_message(stranger, "hello there"),
        state.clone(),
    )
    .await
    .expect("handle_text succeeds");
    let routed = wait_for(
        || {
            tg.sent_messages()
                .iter()
                .any(|m| m.chat_id == stranger && m.text == "echo: hello there")
        },
        Duration::from_secs(5),
    )
    .await;
    assert!(
        routed,
        "authorized turn should route to opencode, got {:?}",
        tg.sent_messages()
    );
}

/// `pair approve` rejects an unknown code and an expired code with clear errors.
#[tokio::test]
async fn pair_approve_rejects_unknown_and_expired_codes() {
    let oc = MockOpencode::start().await;
    let tg = MockTelegram::start().await;
    let state = state_with_bot(config_unpaired(&oc.url), bot_pointed_at(&tg)).await;
    let db = state.db.clone();

    let dir = tempfile::tempdir().expect("tempdir");
    let socket = dir.path().join("admin.sock");
    let admin_state: Arc<dyn AdminState> = state.clone();
    let server = tokio::spawn(admin::serve_admin(admin_state, socket.clone()));

    // Unknown code → error.
    let req = AdminRequest::PairApprove {
        code: "000000".to_string(),
        slot: "you".to_string(),
    };
    match admin_send(&socket, &req).await {
        AdminResponse::Error { message } => {
            assert!(message.contains("no pending pairing"), "got: {message}");
        }
        other => panic!("expected Error, got {other:?}"),
    }

    // Insert an already-expired pending row directly, then approve → error.
    use telegram_opencode_proxy::persistence::PendingPairing;
    db.insert_pairing(&PendingPairing {
        code: "111111".to_string(),
        chat_id: 777,
        username: None,
        expires_at: 1, // epoch second 1 — long past.
    })
    .unwrap();
    let req = AdminRequest::PairApprove {
        code: "111111".to_string(),
        slot: "you".to_string(),
    };
    match admin_send(&socket, &req).await {
        AdminResponse::Error { message } => {
            assert!(message.contains("expired"), "got: {message}");
        }
        other => panic!("expected Error, got {other:?}"),
    }
    // Nobody was bound by either failed approval.
    assert_eq!(db.allowed_slot(777).unwrap(), None);
    server.abort();
}

/// `pair deny` drops a pending request; denying a second time reports nothing.
#[tokio::test]
async fn pair_deny_drops_a_pending_request() {
    let oc = MockOpencode::start().await;
    let tg = MockTelegram::start().await;
    let state = state_with_bot(config_unpaired(&oc.url), bot_pointed_at(&tg)).await;
    let db = state.db.clone();

    // A stranger requests a code.
    let bot = bot_pointed_at(&tg);
    handle_text(bot, text_message(888, "hi"), state.clone())
        .await
        .expect("handle_text succeeds");
    let code = extract_code(&tg.sent_messages()[0].text);

    let dir = tempfile::tempdir().expect("tempdir");
    let socket = dir.path().join("admin.sock");
    let admin_state: Arc<dyn AdminState> = state.clone();
    let server = tokio::spawn(admin::serve_admin(admin_state, socket.clone()));

    let req = AdminRequest::PairDeny { code: code.clone() };
    match admin_send(&socket, &req).await {
        AdminResponse::PairDeny { removed, .. } => assert!(removed, "deny must drop the row"),
        other => panic!("expected PairDeny, got {other:?}"),
    }
    assert!(db.pairing_by_code(&code).unwrap().is_none());

    // Denying again reports nothing was removed.
    match admin_send(&socket, &AdminRequest::PairDeny { code }).await {
        AdminResponse::PairDeny { removed, .. } => assert!(!removed),
        other => panic!("expected PairDeny, got {other:?}"),
    }
    server.abort();
}

/// Poll `pred` every 10ms until it holds or `budget` elapses; returns whether it
/// held. Used to await the background slot bring-up without a fixed sleep.
async fn wait_for(mut pred: impl FnMut() -> bool, budget: Duration) -> bool {
    let start = std::time::Instant::now();
    while start.elapsed() < budget {
        if pred() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    pred()
}

/// Non-blocking startup (#51): `spawn_slot_bringup` returns immediately with an
/// empty registry, and a reachable slot connects into it in the background.
#[tokio::test]
async fn slots_connect_in_the_background() {
    let oc = MockOpencode::start().await;
    let cfg = config_for(&oc.url); // slot "you" bound to SLOT_ID
    let db = Db::open_in_memory().expect("in-memory persistence store opens");
    let bot = Bot::new("12345:test-token");

    // Exactly what `serve` now constructs: an empty registry.
    let state = AppState::new(cfg, "unused.toml".into(), HashMap::new(), db, bot);

    // The whitelist is seeded from `cfg.slots`, so the bound user is authorized
    // immediately — before any slot has connected.
    assert!(
        state
            .db
            .list_allowed()
            .expect("list_allowed")
            .contains(&(SLOT_ID, "you".to_string())),
        "bound user must be whitelisted regardless of connection state"
    );
    // Registry starts empty — the dispatcher would already be live here.
    assert!(state.registry.read().expect("registry lock").is_empty());

    spawn_slot_bringup(Arc::clone(&state), 120, Duration::from_millis(20));

    let connected = wait_for(
        || {
            state
                .registry
                .read()
                .expect("registry lock")
                .contains_key("you")
        },
        Duration::from_secs(5),
    )
    .await;
    assert!(connected, "reachable slot should connect in the background");
}

/// An unreachable slot's bring-up runs in its own task: it never blocks (spawn
/// returns at once) and never populates the registry — but the bound user is
/// still whitelisted so they can be served the moment the slot recovers.
#[tokio::test]
async fn dead_slot_bringup_does_not_block_or_populate() {
    // Port 1 is unreachable → readiness fails fast (connection refused).
    let cfg = config_for("http://127.0.0.1:1");
    let db = Db::open_in_memory().expect("in-memory persistence store opens");
    let bot = Bot::new("12345:test-token");
    let state = AppState::new(cfg, "unused.toml".into(), HashMap::new(), db, bot);

    assert!(
        state
            .db
            .list_allowed()
            .expect("list_allowed")
            .contains(&(SLOT_ID, "you".to_string())),
        "bound user is whitelisted even though the slot never connects"
    );

    // A tiny readiness budget so the failing task gives up quickly.
    spawn_slot_bringup(Arc::clone(&state), 2, Duration::from_millis(5));

    // spawn_slot_bringup returned immediately (it is not `async`); give the
    // background task time to exhaust its budget, then confirm it stayed absent.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        state.registry.read().expect("registry lock").is_empty(),
        "an unreachable slot must not populate the registry"
    );
}

// --- B3 per-user serialization + /stop (#9) -----------------------------------

/// Two messages from one user run through the same serial worker: both complete,
/// and in FIFO order (never interleaved).
#[tokio::test]
async fn same_user_turns_are_serialized_in_order() {
    let oc = MockOpencode::start().await;
    let tg = MockTelegram::start().await;
    let state = state_for(config_for(&oc.url)).await;
    let bot = bot_pointed_at(&tg);

    handle_text(bot.clone(), text_message(SLOT_ID, "one"), state.clone())
        .await
        .expect("enqueue one");
    handle_text(bot.clone(), text_message(SLOT_ID, "two"), state.clone())
        .await
        .expect("enqueue two");

    let both = wait_for(
        || {
            tg.sent_messages()
                .iter()
                .filter(|m| m.text.starts_with("echo:"))
                .count()
                >= 2
        },
        Duration::from_secs(5),
    )
    .await;
    assert!(
        both,
        "both turns should complete, got {:?}",
        tg.sent_messages()
    );

    let echoes: Vec<String> = tg
        .sent_messages()
        .into_iter()
        .map(|m| m.text)
        .filter(|t| t.starts_with("echo:"))
        .collect();
    assert_eq!(
        echoes,
        vec!["echo: one".to_string(), "echo: two".to_string()],
        "turns must run in FIFO order, not interleaved"
    );
}

/// When a user's bounded queue is full (worker stuck on a slow turn), further
/// messages are rejected with a busy reply — the dispatcher is never blocked.
#[tokio::test]
async fn a_full_queue_rejects_with_a_busy_message() {
    let oc = MockOpencode::start().await;
    let tg = MockTelegram::start().await;
    // Hold the in-flight turn open so the queue stays full while we pile on.
    oc.set_message_delay(Duration::from_secs(3));
    let state = state_for(config_for(&oc.url)).await;
    let bot = bot_pointed_at(&tg);

    // Far more than the queue depth: the worker is stuck on the first turn, so
    // the queue fills and the overflow is rejected synchronously by handle_text.
    for i in 0..20 {
        handle_text(
            bot.clone(),
            text_message(SLOT_ID, &format!("m{i}")),
            state.clone(),
        )
        .await
        .expect("handle_text succeeds");
    }

    let busy = tg
        .sent_messages()
        .into_iter()
        .filter(|m| m.text.contains("still working"))
        .count();
    assert!(
        busy >= 1,
        "a full queue must reject with a busy reply, got {:?}",
        tg.sent_messages()
    );
}

/// `/stop` with a live session → `POST /session/:id/abort` and a confirmation.
#[tokio::test]
async fn stop_aborts_the_users_session() {
    let oc = MockOpencode::start().await;
    let tg = MockTelegram::start().await;
    let state = state_for(config_for(&oc.url)).await;
    // Give the user a live session for /stop to abort.
    state
        .db
        .set_session(SLOT_ID, "ses_live")
        .expect("set_session");
    let bot = bot_pointed_at(&tg);

    handle_stop(bot, text_message(SLOT_ID, "/stop"), state)
        .await
        .expect("handle_stop succeeds");

    assert_eq!(
        oc.aborted_sessions(),
        vec!["ses_live".to_string()],
        "the user's session must be aborted"
    );
    assert!(
        tg.sent_messages()
            .iter()
            .any(|m| m.chat_id == SLOT_ID && m.text.contains("Stopped")),
        "expected a stop confirmation, got {:?}",
        tg.sent_messages()
    );
}

/// `/stop` with no session → "Nothing to stop." and no abort call.
#[tokio::test]
async fn stop_with_no_session_is_a_no_op() {
    let oc = MockOpencode::start().await;
    let tg = MockTelegram::start().await;
    let state = state_for(config_for(&oc.url)).await;
    let bot = bot_pointed_at(&tg);

    handle_stop(bot, text_message(SLOT_ID, "/stop"), state)
        .await
        .expect("handle_stop succeeds");

    assert!(
        oc.aborted_sessions().is_empty(),
        "no session → no abort call"
    );
    assert!(
        tg.sent_messages()
            .iter()
            .any(|m| m.text.contains("Nothing to stop")),
        "expected a nothing-to-stop reply, got {:?}",
        tg.sent_messages()
    );
}

// --- Telegram error / rate-limit / backoff (#25) ------------------------------

/// A 429 flood-control response is retried after the server's `retry_after`,
/// and the call ultimately succeeds.
#[tokio::test]
async fn telegram_send_recovers_from_flood_control() {
    let tg = MockTelegram::start().await;
    tg.fail_next_429(1); // first attempt floods, second succeeds
    let bot = bot_pointed_at(&tg);

    let res = telegram_opencode_proxy::telegram::retry::with_retry("send", || {
        let bot = bot.clone();
        async move { bot.send_message(ChatId(7), "hi").await }
    })
    .await;

    assert!(res.is_ok(), "retry should recover from a 429, got {res:?}");
    assert_eq!(tg.send_attempts(), 2, "one 429 then one success");
    assert!(tg.sent_messages().iter().any(|m| m.text == "hi"));
}

/// A non-transient 400 is NOT retried — the wrapper returns after one attempt.
#[tokio::test]
async fn telegram_send_does_not_retry_bad_request() {
    let tg = MockTelegram::start().await;
    tg.fail_next_400(3); // would fail three times, but we must only try once
    let bot = bot_pointed_at(&tg);

    let res = telegram_opencode_proxy::telegram::retry::with_retry("send", || {
        let bot = bot.clone();
        async move { bot.send_message(ChatId(7), "hi").await }
    })
    .await;

    assert!(res.is_err(), "a 400 must not be retried");
    assert_eq!(
        tg.send_attempts(),
        1,
        "a non-transient error must not be retried"
    );
}

// --- Graceful shutdown (#21) --------------------------------------------------

/// An in-flight turn finishes during graceful shutdown: `AppState::shutdown`
/// drains the user's worker, so the reply is delivered before it returns (no
/// polling — the drain awaited the worker).
#[tokio::test]
async fn shutdown_drains_in_flight_turns() {
    let oc = MockOpencode::start().await;
    let tg = MockTelegram::start().await;
    let state = state_for(config_for(&oc.url)).await;
    let bot = bot_pointed_at(&tg);

    // Enqueue a turn (spawns the user's worker), then shut down immediately.
    handle_text(bot, text_message(SLOT_ID, "hello"), state.clone())
        .await
        .expect("handle_text succeeds");
    state.shutdown(Duration::from_secs(5)).await;

    assert!(
        tg.sent_messages().iter().any(|m| m.text == "echo: hello"),
        "in-flight turn should finish during graceful shutdown, got {:?}",
        tg.sent_messages()
    );
}

// --- opencode reachability (#22) ----------------------------------------------

/// A turn whose slot's opencode is unreachable gets the clear "unreachable"
/// message, not the generic error.
#[tokio::test]
async fn unreachable_opencode_gets_a_clear_message() {
    let tg = MockTelegram::start().await;
    // A slot that is live in the registry but whose opencode URL refuses
    // connections (port 1) — i.e. it went down after startup.
    let dead = "http://127.0.0.1:1";
    let cfg = config_for(dead);
    let client = OpencodeClient::new(dead).expect("client");
    let mut registry = HashMap::new();
    registry.insert(
        "you".to_string(),
        SlotConn {
            slot: cfg.slots[0].clone(),
            client,
        },
    );
    let db = Db::open_in_memory().expect("db");
    let bot = bot_pointed_at(&tg);
    let state = AppState::new(cfg, "unused.toml".into(), registry, db, bot.clone());

    handle_text(bot, text_message(SLOT_ID, "hi"), state.clone())
        .await
        .expect("handle_text succeeds");

    let got = wait_for(
        || {
            tg.sent_messages()
                .iter()
                .any(|m| m.text.contains("unreachable"))
        },
        Duration::from_secs(5),
    )
    .await;
    assert!(
        got,
        "expected an 'opencode unreachable' reply, got {:?}",
        tg.sent_messages()
    );
}

// --- B4 commands: /new + verbosity (#10) --------------------------------------

/// `/new` clears the user's stored session so the next turn starts fresh.
#[tokio::test]
async fn new_command_resets_the_session() {
    let oc = MockOpencode::start().await;
    let tg = MockTelegram::start().await;
    let state = state_for(config_for(&oc.url)).await;
    state
        .db
        .set_session(SLOT_ID, "ses_old")
        .expect("seed a session");
    let bot = bot_pointed_at(&tg);

    handle_new(bot, text_message(SLOT_ID, "/new"), state.clone())
        .await
        .expect("handle_new succeeds");

    assert!(
        state.db.get_session(SLOT_ID).unwrap().is_none(),
        "the session must be cleared"
    );
    assert!(
        tg.sent_messages()
            .iter()
            .any(|m| m.text.contains("fresh session")),
        "expected a fresh-session confirmation, got {:?}",
        tg.sent_messages()
    );
}

/// `/quiet` toggles: first sets Quiet, a second returns to Normal.
#[tokio::test]
async fn quiet_command_toggles_verbosity() {
    let oc = MockOpencode::start().await;
    let tg = MockTelegram::start().await;
    let state = state_for(config_for(&oc.url)).await;
    let bot = bot_pointed_at(&tg);

    handle_verbosity(
        bot.clone(),
        text_message(SLOT_ID, "/quiet"),
        state.clone(),
        Verbosity::Quiet,
    )
    .await
    .expect("handle_verbosity");
    assert_eq!(state.db.get_verbosity(SLOT_ID).unwrap(), Verbosity::Quiet);

    handle_verbosity(
        bot,
        text_message(SLOT_ID, "/quiet"),
        state.clone(),
        Verbosity::Quiet,
    )
    .await
    .expect("handle_verbosity");
    assert_eq!(
        state.db.get_verbosity(SLOT_ID).unwrap(),
        Verbosity::Normal,
        "a second /quiet returns to Normal"
    );
}
