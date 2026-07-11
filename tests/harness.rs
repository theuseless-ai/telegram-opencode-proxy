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

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use serde_json::json;
use teloxide::Bot;
use teloxide::types::Message;

use telegram_opencode_proxy::admin::{self, AdminRequest, AdminResponse, AdminState, SlotSource};
use telegram_opencode_proxy::config::{Config, Model, Pairing, Permissions, Slot};
use telegram_opencode_proxy::connect_slots;
use telegram_opencode_proxy::persistence::Db;
use telegram_opencode_proxy::telegram::bot::{AppState, handle_text};

use support::mock_opencode::MockOpencode;
use support::mock_telegram::MockTelegram;

const SLOT_ID: i64 = 111;

/// Build a single-slot config pointing at `opencode_url`, bound to `SLOT_ID`.
fn config_for(opencode_url: &str) -> Config {
    Config {
        bot_token: "12345:test-token".to_string(),
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
    AppState::new(cfg, registry, db, bot)
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

    let sent = tg.sent_messages();
    assert_eq!(sent.len(), 1, "exactly one reply expected, got {sent:?}");
    assert_eq!(sent[0].chat_id, SLOT_ID);
    // The mock echoes the prompt, so the reply must carry the prompt text.
    assert_eq!(sent[0].text, "echo: hello there");
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
    let total: usize = sent.iter().map(|m| m.text.chars().count()).sum();
    assert_eq!(total, 9000, "chunks must reconstruct the full reply length");
}

/// 4. Provider-validation failure → `serve`'s bring-up (`connect_slots`) errors
///    with a clear, actionable message naming the missing model.
#[tokio::test]
async fn provider_validation_failure_is_reported_clearly() {
    let oc = MockOpencode::start_without_model().await;
    let cfg = config_for(&oc.url);

    let err = connect_slots(&cfg)
        .await
        .expect_err("bring-up must fail when the model is absent from the catalogue");
    let msg = format!("{err:#}");

    assert!(
        msg.contains("Qwen3.6-35B-A3B-bf16"),
        "error should name the missing model, got: {msg}"
    );
    assert!(
        msg.contains("validating model for slot 'you'"),
        "error should identify the failing slot, got: {msg}"
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
    AppState::new(cfg, registry, db, bot)
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
            assert_eq!(you.source, SlotSource::Config);
        }
        other => panic!("expected Slots, got {other:?}"),
    }
    server.abort();

    // 4. The same sender is now authorized → routes to opencode.
    handle_text(
        bot.clone(),
        text_message(stranger, "hello there"),
        state.clone(),
    )
    .await
    .expect("handle_text succeeds");
    let sent = tg.sent_messages();
    let last = sent.last().expect("at least one message");
    assert_eq!(last.chat_id, stranger);
    assert_eq!(last.text, "echo: hello there");
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
