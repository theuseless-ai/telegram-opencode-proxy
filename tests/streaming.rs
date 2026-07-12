//! Integration coverage for the streaming turn driver (issue #8, B2).
//!
//! Drives the REAL [`run_streaming_turn`] against both in-process mocks over an
//! actual HTTP+SSE round-trip: `mock_opencode` trickles `/global/event` deltas
//! while holding the blocking `POST /message` open, and `mock_telegram` records
//! the resulting `editMessageText` / `sendChatAction` / `sendMessage` calls.
//!
//! It proves the B2 behaviours the `LiveState` unit tests can't reach without a
//! wire: text deltas drive **live edits**, `typing` liveness is sent, reasoning
//! deltas and other sessions' deltas are **not** rendered, and the message is
//! finalized to the authoritative assistant text.

#[path = "support/mock_opencode.rs"]
#[allow(dead_code)]
mod mock_opencode;
#[path = "support/mock_telegram.rs"]
#[allow(dead_code)]
mod mock_telegram;

use std::time::Duration;

use teloxide::Bot;

use telegram_opencode_proxy::opencode::client::OpencodeClient;
use telegram_opencode_proxy::opencode::types::PromptModel;
use telegram_opencode_proxy::telegram::render::Verbosity;
use telegram_opencode_proxy::telegram::stream::{StreamTiming, run_streaming_turn};

use mock_opencode::MockOpencode;
use mock_telegram::MockTelegram;

const CHAT_ID: i64 = 111;
const SESSION: &str = "ses_stream";

/// One paced SSE `data:` frame from a JSON payload value.
fn frame(payload: serde_json::Value) -> String {
    format!("data: {}\n\n", serde_json::json!({ "payload": payload }))
}

fn delta(session: &str, part_id: &str, text: &str) -> serde_json::Value {
    serde_json::json!({
        "type": "message.part.delta",
        "properties": {
            "sessionID": session, "messageID": "msg_a",
            "partID": part_id, "field": "text", "delta": text
        }
    })
}

fn part_updated(session: &str, part_id: &str, ptype: &str) -> serde_json::Value {
    serde_json::json!({
        "type": "message.part.updated",
        "properties": {
            "sessionID": session,
            "part": { "id": part_id, "messageID": "msg_a", "sessionID": session, "type": ptype }
        }
    })
}

/// The full stream text seen across all writes (creates + edits), so assertions
/// can check what did / didn't reach Telegram regardless of edit timing.
fn all_text(tg: &MockTelegram) -> Vec<String> {
    let mut out: Vec<String> = tg.sent_messages().into_iter().map(|m| m.text).collect();
    out.extend(tg.edits().into_iter().map(|e| e.text));
    out
}

#[tokio::test]
async fn streaming_turn_live_edits_typing_and_finalizes() {
    let oc = MockOpencode::start().await;
    let tg = MockTelegram::start().await;

    // Authoritative reply differs from the streamed text so a finalize edit is
    // always observable regardless of flush timing.
    oc.set_reply("Hello world!");
    // Hold the blocking POST open so the paced stream + edits play out first.
    oc.set_message_delay(Duration::from_millis(300));
    oc.set_event_frames(
        vec![
            frame(serde_json::json!({"type":"server.connected","properties":{}})),
            // Reasoning is announced then streamed — must be treated as liveness
            // only, never rendered into the answer.
            frame(part_updated(SESSION, "prt_reason", "reasoning")),
            frame(delta(SESSION, "prt_reason", "thinking about it")),
            // A delta for a DIFFERENT session must be ignored.
            frame(delta("ses_other", "prt_x", "SHOULD_NOT_APPEAR")),
            // The real answer streams in two chunks.
            frame(part_updated(SESSION, "prt_ans", "text")),
            frame(delta(SESSION, "prt_ans", "Hello ")),
            frame(delta(SESSION, "prt_ans", "world")),
        ],
        Duration::from_millis(20),
    );

    let bot = Bot::new("test-token").set_api_url(tg.url.parse().expect("mock url"));
    let http = reqwest::Client::new();
    let client = OpencodeClient::new(&oc.url).expect("client");
    let model = PromptModel {
        provider_id: "llm-lan".into(),
        model_id: "Qwen3.6-35B-A3B-bf16".into(),
    };
    let timing = StreamTiming {
        flush_interval: Duration::from_millis(10),
        typing_interval: Duration::from_millis(20),
        retry: Duration::from_secs(5), // >> the 300ms turn: no mid-turn reconnect
    };

    run_streaming_turn(
        &bot,
        &http,
        &client,
        &oc.url,
        CHAT_ID,
        SESSION,
        model,
        "hi",
        Verbosity::Normal,
        timing,
    )
    .await
    .expect("streaming turn");

    let texts = all_text(&tg);
    assert!(!texts.is_empty(), "the turn wrote at least one message");

    // Liveness: at least one live edit happened (message created then edited).
    assert!(
        !tg.edits().is_empty(),
        "expected live editMessageText calls, got none"
    );
    // `typing` was sent for ambient liveness.
    assert!(
        tg.chat_actions().iter().any(|a| a == "typing"),
        "expected a typing chat action, got {:?}",
        tg.chat_actions()
    );

    // Reasoning text and the other session's delta never reach Telegram.
    assert!(
        texts.iter().all(|t| !t.contains("thinking about it")),
        "reasoning text must not be rendered: {texts:?}"
    );
    assert!(
        texts.iter().all(|t| !t.contains("SHOULD_NOT_APPEAR")),
        "another session's delta must not be rendered: {texts:?}"
    );

    // The final visible message is the authoritative reply.
    let final_text = tg
        .edits()
        .last()
        .map(|e| e.text.clone())
        .or_else(|| tg.sent_messages().last().map(|m| m.text.clone()))
        .expect("some message was written");
    assert_eq!(final_text, "Hello world!");

    // Everything went to the right chat.
    assert!(tg.sent_messages().iter().all(|m| m.chat_id == CHAT_ID));
    assert!(tg.edits().iter().all(|e| e.chat_id == CHAT_ID));
}

#[tokio::test]
async fn leading_whitespace_delta_does_not_trigger_empty_send() {
    // Regression: a model whose first streamed token is whitespace briefly makes
    // the live buffer whitespace-only, which Telegram rejects ("text must be
    // non-empty"). The driver must hold off until there is a visible character.
    let oc = MockOpencode::start().await;
    let tg = MockTelegram::start().await;
    oc.set_reply("Hi there");
    oc.set_message_delay(Duration::from_millis(250));
    oc.set_event_frames(
        vec![
            frame(serde_json::json!({"type":"server.connected","properties":{}})),
            frame(part_updated(SESSION, "prt_ans", "text")),
            // First token is whitespace only — must NOT create a message.
            frame(delta(SESSION, "prt_ans", "   ")),
            // Then real text arrives.
            frame(delta(SESSION, "prt_ans", "Hi there")),
        ],
        Duration::from_millis(20),
    );

    let bot = Bot::new("test-token").set_api_url(tg.url.parse().expect("mock url"));
    let http = reqwest::Client::new();
    let client = OpencodeClient::new(&oc.url).expect("client");
    let model = PromptModel {
        provider_id: "llm-lan".into(),
        model_id: "Qwen3.6-35B-A3B-bf16".into(),
    };

    run_streaming_turn(
        &bot,
        &http,
        &client,
        &oc.url,
        CHAT_ID,
        SESSION,
        model,
        "hi",
        Verbosity::Normal,
        StreamTiming {
            flush_interval: Duration::from_millis(10),
            typing_interval: Duration::from_millis(20),
            retry: Duration::from_secs(5),
        },
    )
    .await
    .expect("streaming turn");

    // The driver never attempted a whitespace-only send/edit.
    assert_eq!(
        tg.empty_rejections(),
        0,
        "driver sent a whitespace-only body; Telegram would reject it"
    );
    // No recorded write is whitespace-only, and the reply still arrives.
    let texts = all_text(&tg);
    assert!(texts.iter().all(|t| !t.trim().is_empty()));
    assert!(
        texts.iter().any(|t| t.contains("Hi there")),
        "final reply must be delivered, got {texts:?}"
    );
}

#[tokio::test]
async fn streaming_turn_with_no_stream_still_posts_final_reply() {
    // No event frames set → `/global/event` just emits `server.connected` and
    // closes; the turn should still deliver the blocking reply as one message.
    let oc = MockOpencode::start().await;
    let tg = MockTelegram::start().await;
    oc.set_reply("PONG");

    let bot = Bot::new("test-token").set_api_url(tg.url.parse().expect("mock url"));
    let http = reqwest::Client::new();
    let client = OpencodeClient::new(&oc.url).expect("client");
    let model = PromptModel {
        provider_id: "llm-lan".into(),
        model_id: "Qwen3.6-35B-A3B-bf16".into(),
    };

    run_streaming_turn(
        &bot,
        &http,
        &client,
        &oc.url,
        CHAT_ID,
        SESSION,
        model,
        "ping",
        Verbosity::Normal,
        StreamTiming {
            flush_interval: Duration::from_millis(10),
            typing_interval: Duration::from_millis(20),
            retry: Duration::from_secs(5),
        },
    )
    .await
    .expect("streaming turn");

    let texts = all_text(&tg);
    assert!(
        texts.iter().any(|t| t == "PONG"),
        "final reply must be delivered, got {texts:?}"
    );
}
