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
use std::sync::Arc;

use serde_json::json;
use teloxide::Bot;
use teloxide::types::Message;

use telegram_opencode_proxy::config::{Config, Model, Permissions, Slot};
use telegram_opencode_proxy::connect_slots;
use telegram_opencode_proxy::opencode::client::OpencodeClient;
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

/// Bring up the real per-slot clients for `cfg` against the running mock.
async fn state_for(cfg: Config) -> Arc<AppState> {
    let clients: HashMap<String, OpencodeClient> = connect_slots(&cfg)
        .await
        .expect("connect_slots (readiness + model validation) succeeds against the mock");
    // In-memory routing store — hermetic, no file touched by the harness.
    let db = Db::open_in_memory().expect("in-memory persistence store opens");
    AppState::new(cfg, clients, db)
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
