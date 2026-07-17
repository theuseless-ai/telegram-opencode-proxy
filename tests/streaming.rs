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
use telegram_opencode_proxy::opencode::types::{PartInput, PromptModel};
use telegram_opencode_proxy::telegram::render::Verbosity;
use telegram_opencode_proxy::telegram::stream::{StreamTiming, run_streaming_turn};

use mock_opencode::MockOpencode;
use mock_telegram::MockTelegram;

const CHAT_ID: i64 = 111;
const SESSION: &str = "ses_stream";

fn text_parts(s: &str) -> Vec<PartInput> {
    vec![PartInput::Text {
        text: s.to_string(),
    }]
}

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
    let db = telegram_opencode_proxy::persistence::Db::open_in_memory().expect("db");
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
        &db,
        &oc.url,
        CHAT_ID,
        SESSION,
        model,
        text_parts("hi"),
        Verbosity::Normal,
        None,
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

    // The final visible message is the authoritative reply, rendered to
    // MarkdownV2 — the `!` is a reserved char and must arrive backslash-escaped
    // (#70), sent with `parse_mode=MarkdownV2`.
    let final_msg = tg
        .edits()
        .last()
        .map(|e| (e.text.clone(), e.parse_mode.clone()))
        .or_else(|| {
            tg.sent_messages()
                .last()
                .map(|m| (m.text.clone(), m.parse_mode.clone()))
        })
        .expect("some message was written");
    assert_eq!(
        final_msg.0, "Hello world\\!",
        "reply rendered to MarkdownV2"
    );
    assert_eq!(
        final_msg.1.as_deref(),
        Some("MarkdownV2"),
        "the formatted send must carry parse_mode=MarkdownV2"
    );

    // Everything went to the right chat.
    assert!(tg.sent_messages().iter().all(|m| m.chat_id == CHAT_ID));
    assert!(tg.edits().iter().all(|e| e.chat_id == CHAT_ID));
}

#[tokio::test]
async fn finalize_footer_shows_context_usage_percent() {
    // With the model's context window known and the reply reporting token usage,
    // the completion footer surfaces a context-used % — even with no tools (#72).
    let oc = MockOpencode::start().await;
    let tg = MockTelegram::start().await;
    oc.set_reply("Hello");
    oc.set_reply_tokens(50_000); // 50k of a 100k window → 50%
    oc.set_message_delay(Duration::from_millis(150));
    oc.set_event_frames(
        vec![
            frame(serde_json::json!({"type":"server.connected","properties":{}})),
            frame(part_updated(SESSION, "prt_ans", "text")),
            frame(delta(SESSION, "prt_ans", "Hello")),
        ],
        Duration::from_millis(20),
    );

    let bot = Bot::new("test-token").set_api_url(tg.url.parse().expect("mock url"));
    let http = reqwest::Client::new();
    let client = OpencodeClient::new(&oc.url).expect("client");
    let db = telegram_opencode_proxy::persistence::Db::open_in_memory().expect("db");
    let model = PromptModel {
        provider_id: "llm-lan".into(),
        model_id: "Qwen3.6-35B-A3B-bf16".into(),
    };

    run_streaming_turn(
        &bot,
        &http,
        &client,
        &db,
        &oc.url,
        CHAT_ID,
        SESSION,
        model,
        text_parts("hi"),
        Verbosity::Normal,
        Some(100_000), // context window → drives the %
        StreamTiming {
            flush_interval: Duration::from_millis(10),
            typing_interval: Duration::from_millis(20),
            retry: Duration::from_secs(5),
        },
    )
    .await
    .expect("streaming turn");

    // The finalized message ends with the context-usage footer (footer last, #6).
    let final_text = tg
        .edits()
        .last()
        .map(|e| e.text.clone())
        .or_else(|| tg.sent_messages().last().map(|m| m.text.clone()))
        .expect("some message was written");
    assert_eq!(final_text, "Hello\n🧠 50%", "footer shows context %, no ✓");
}

/// A `message.part.updated` tool frame in its wire shape (state.status/title).
fn tool_part_updated(
    session: &str,
    part_id: &str,
    call_id: &str,
    status: &str,
    title: &str,
) -> serde_json::Value {
    serde_json::json!({
        "type": "message.part.updated",
        "properties": {
            "sessionID": session,
            "part": {
                "id": part_id, "messageID": "msg_a", "sessionID": session,
                "type": "tool", "tool": "bash", "callID": call_id,
                "state": { "status": status, "title": title }
            }
        }
    })
}

#[tokio::test]
async fn verbose_finalize_folds_the_activity_log_into_an_expandable_blockquote() {
    // At Verbose the tool activity of the turn is folded into a collapsed
    // MarkdownV2 expandable blockquote above the answer, footer last (#6).
    let oc = MockOpencode::start().await;
    let tg = MockTelegram::start().await;
    oc.set_reply("Done");
    oc.set_message_delay(Duration::from_millis(150));
    oc.set_event_frames(
        vec![
            frame(serde_json::json!({"type":"server.connected","properties":{}})),
            frame(tool_part_updated(
                SESSION,
                "prt_tool",
                "call_1",
                "running",
                "git status",
            )),
            frame(tool_part_updated(
                SESSION,
                "prt_tool",
                "call_1",
                "completed",
                "git status",
            )),
        ],
        Duration::from_millis(20),
    );

    let bot = Bot::new("test-token").set_api_url(tg.url.parse().expect("mock url"));
    let http = reqwest::Client::new();
    let client = OpencodeClient::new(&oc.url).expect("client");
    let db = telegram_opencode_proxy::persistence::Db::open_in_memory().expect("db");
    let model = PromptModel {
        provider_id: "llm-lan".into(),
        model_id: "Qwen3.6-35B-A3B-bf16".into(),
    };

    run_streaming_turn(
        &bot,
        &http,
        &client,
        &db,
        &oc.url,
        CHAT_ID,
        SESSION,
        model,
        text_parts("hi"),
        Verbosity::Verbose,
        None,
        StreamTiming {
            flush_interval: Duration::from_millis(10),
            typing_interval: Duration::from_millis(20),
            retry: Duration::from_secs(5),
        },
    )
    .await
    .expect("streaming turn");

    let final_text = tg
        .edits()
        .last()
        .map(|e| e.text.clone())
        .or_else(|| tg.sent_messages().last().map(|m| m.text.clone()))
        .expect("some message was written");
    assert_eq!(
        final_text, "**>🔧 1 tool\n>✓ bash · git status||\n\nDone\n✓ 1 tool",
        "collapsed log rides above the answer, footer last"
    );
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
    let db = telegram_opencode_proxy::persistence::Db::open_in_memory().expect("db");
    let model = PromptModel {
        provider_id: "llm-lan".into(),
        model_id: "Qwen3.6-35B-A3B-bf16".into(),
    };

    run_streaming_turn(
        &bot,
        &http,
        &client,
        &db,
        &oc.url,
        CHAT_ID,
        SESSION,
        model,
        text_parts("hi"),
        Verbosity::Normal,
        None,
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
    let db = telegram_opencode_proxy::persistence::Db::open_in_memory().expect("db");
    let model = PromptModel {
        provider_id: "llm-lan".into(),
        model_id: "Qwen3.6-35B-A3B-bf16".into(),
    };

    run_streaming_turn(
        &bot,
        &http,
        &client,
        &db,
        &oc.url,
        CHAT_ID,
        SESSION,
        model,
        text_parts("ping"),
        Verbosity::Normal,
        None,
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

/// A `permission.asked` frame during a turn posts the approval buttons and
/// stores a pending-approval row (#13).
#[tokio::test]
async fn permission_asked_posts_approval_buttons() {
    let oc = MockOpencode::start().await;
    let tg = MockTelegram::start().await;
    oc.set_reply("done");
    // Hold the blocking POST so the permission frame streams first.
    oc.set_message_delay(Duration::from_millis(300));
    oc.set_event_frames(
        vec![
            frame(serde_json::json!({"type":"server.connected","properties":{}})),
            frame(serde_json::json!({
                "type": "permission.asked",
                "properties": {
                    "id": "per_stream",
                    "sessionID": SESSION,
                    "permission": "bash",
                    "patterns": ["git commit -m x"],
                    "metadata": { "command": "git commit -m x" },
                    "tool": { "messageID": "msg_a", "callID": "call_1" }
                }
            })),
        ],
        Duration::from_millis(20),
    );

    let bot = Bot::new("test-token").set_api_url(tg.url.parse().expect("mock url"));
    let http = reqwest::Client::new();
    let client = OpencodeClient::new(&oc.url).expect("client");
    let db = telegram_opencode_proxy::persistence::Db::open_in_memory().expect("db");
    let model = PromptModel {
        provider_id: "llm-lan".into(),
        model_id: "Qwen3.6-35B-A3B-bf16".into(),
    };

    run_streaming_turn(
        &bot,
        &http,
        &client,
        &db,
        &oc.url,
        CHAT_ID,
        SESSION,
        model,
        text_parts("commit please"),
        Verbosity::Normal,
        None,
        StreamTiming {
            flush_interval: Duration::from_millis(10),
            typing_interval: Duration::from_millis(20),
            retry: Duration::from_secs(5),
        },
    )
    .await
    .expect("streaming turn");

    // A permission prompt with an inline keyboard was posted.
    assert!(
        tg.sent_messages().iter().any(|m| m.text.contains("🔐")),
        "expected a permission prompt, got {:?}",
        tg.sent_messages()
    );
    assert!(
        tg.markup_messages() >= 1,
        "the prompt should carry an inline keyboard"
    );
    // And the gate was recorded for the callback to answer.
    assert!(
        db.list_approvals()
            .unwrap()
            .iter()
            .any(|a| a.session_id == SESSION && a.token == "per_stream"),
        "an approval row should be stored"
    );
}

/// Drive one turn against a canned set of `permission.asked` frames and return
/// `(mock telegram, db)` for assertions. `setup` registers whatever session
/// parentage the case needs before the turn runs.
async fn permission_turn(
    frames: Vec<serde_json::Value>,
    setup: impl FnOnce(&MockOpencode),
) -> (
    MockOpencode,
    MockTelegram,
    telegram_opencode_proxy::persistence::Db,
) {
    let oc = MockOpencode::start().await;
    let tg = MockTelegram::start().await;
    setup(&oc);
    oc.set_reply("done");
    // Hold the blocking POST open so every gate streams before the turn ends.
    oc.set_message_delay(Duration::from_millis(400));
    let mut sse = vec![frame(serde_json::json!({
        "type": "server.connected", "properties": {}
    }))];
    sse.extend(frames.into_iter().map(frame));
    oc.set_event_frames(sse, Duration::from_millis(20));

    let bot = Bot::new("test-token").set_api_url(tg.url.parse().expect("mock url"));
    let http = reqwest::Client::new();
    let client = OpencodeClient::new(&oc.url).expect("client");
    let db = telegram_opencode_proxy::persistence::Db::open_in_memory().expect("db");
    let model = PromptModel {
        provider_id: "llm-lan".into(),
        model_id: "Qwen3.6-35B-A3B-bf16".into(),
    };

    run_streaming_turn(
        &bot,
        &http,
        &client,
        &db,
        &oc.url,
        CHAT_ID,
        SESSION,
        model,
        text_parts("delegate please"),
        Verbosity::Normal,
        None,
        StreamTiming {
            flush_interval: Duration::from_millis(10),
            typing_interval: Duration::from_millis(20),
            retry: Duration::from_secs(5),
        },
    )
    .await
    .expect("streaming turn");

    (oc, tg, db)
}

/// A `permission.asked` frame for `session`, gating `command`.
fn ask_frame(id: &str, session: &str, command: &str) -> serde_json::Value {
    serde_json::json!({
        "type": "permission.asked",
        "properties": {
            "id": id,
            "sessionID": session,
            "permission": "bash",
            "patterns": [command],
            "metadata": { "command": command },
            "tool": { "messageID": "msg_a", "callID": "call_1" }
        }
    })
}

/// A gate raised by a **Task-spawned subagent** surfaces the same approval
/// buttons the main session gets, named for the asking agent (#88).
///
/// This is the issue's repro: the gate names only the *child* session, so the
/// relay has to walk `parentID` back to the turn. Matching the turn's session id
/// directly dropped it, and the subagent blocked until the delegation failed.
#[tokio::test]
async fn subagent_permission_asked_posts_approval_buttons() {
    let (_oc, tg, db) = permission_turn(vec![ask_frame("per_sub", "ses_child", "df")], |oc| {
        oc.set_subagent_session("ses_child", SESSION, Some("motoko"));
    })
    .await;

    let prompts: Vec<String> = tg
        .sent_messages()
        .iter()
        .filter(|m| m.text.contains("🔐"))
        .map(|m| m.text.clone())
        .collect();
    assert_eq!(
        prompts.len(),
        1,
        "the subagent's gate should post exactly one prompt, got {:?}",
        tg.sent_messages()
    );
    // The asking subagent is named — approving a delegated command shouldn't be
    // indistinguishable from approving the primary agent's own.
    assert!(
        prompts[0].contains("motoko") && prompts[0].contains("df"),
        "the prompt should name the agent and the command, got {:?}",
        prompts[0]
    );
    assert!(
        tg.markup_messages() >= 1,
        "the prompt should carry an inline keyboard"
    );
    // Recorded against the child session, so the callback replies to that gate.
    assert!(
        db.list_approvals()
            .unwrap()
            .iter()
            .any(|a| a.session_id == "ses_child" && a.token == "per_sub"),
        "an approval row should be stored for the subagent's gate"
    );
}

/// A subagent nested under another subagent still resolves to the turn, and the
/// walk is memoized rather than re-run per gate (#88).
#[tokio::test]
async fn nested_subagent_resolves_once_and_is_cached() {
    let (oc, tg, _db) = permission_turn(
        vec![
            ask_frame("per_deep_1", "ses_grandchild", "df"),
            ask_frame("per_deep_2", "ses_grandchild", "du"),
        ],
        |oc| {
            oc.set_subagent_session("ses_child", SESSION, Some("motoko"));
            oc.set_subagent_session("ses_grandchild", "ses_child", Some("batou"));
        },
    )
    .await;

    assert_eq!(
        tg.sent_messages()
            .iter()
            .filter(|m| m.text.contains("🔐"))
            .count(),
        2,
        "both of the grandchild's gates should prompt, got {:?}",
        tg.sent_messages()
    );
    // Parentage is immutable, so the second gate must reuse the first's verdict.
    assert_eq!(
        oc.session_lookups("ses_grandchild"),
        1,
        "the second gate should hit the memoized verdict, not re-walk the chain"
    );
}

/// A chain deeper than the resolver's depth cap is **not** surfaced — and,
/// critically, isn't cached as foreign either, so it doesn't poison a later,
/// shallower gate that could otherwise resolve.
///
/// The relay's `MAX_PARENT_DEPTH` walk gives up after 8 hops. A session 9 hops
/// from the turn's root is beyond that: if the resolver conflated "gave up"
/// with "proven foreign", this gate would be permanently (and wrongly)
/// silenced, and so would any shallower ancestor on the walked path.
#[tokio::test]
async fn deeply_nested_subagent_beyond_depth_cap_is_not_surfaced() {
    let (_oc, tg, db) = permission_turn(vec![ask_frame("per_far", "ses_lvl9", "df")], |oc| {
        // ses_lvl9 -> ses_lvl8 -> ... -> ses_lvl1 -> SESSION: 9 hops to root,
        // one past the 8-hop cap.
        oc.set_subagent_session("ses_lvl1", SESSION, Some("motoko"));
        for lvl in 2..=9 {
            let id = format!("ses_lvl{lvl}");
            let parent = format!("ses_lvl{}", lvl - 1);
            oc.set_subagent_session(&id, &parent, Some("motoko"));
        }
    })
    .await;

    assert!(
        !tg.sent_messages().iter().any(|m| m.text.contains("🔐")),
        "a gate beyond the depth cap must not prompt, got {:?}",
        tg.sent_messages()
    );
    assert!(
        db.list_approvals().unwrap().is_empty(),
        "no approval row should be stored for a gate beyond the depth cap"
    );
}

/// A gate from a session this turn doesn't own is left alone (#88).
///
/// `/global/event` is instance-wide, so every concurrent turn sees this frame.
/// Prompting on it would make another chat's gate answerable here — and have two
/// chats race to reply to one gate.
#[tokio::test]
async fn foreign_session_permission_is_not_surfaced() {
    let (_oc, tg, db) = permission_turn(
        vec![
            ask_frame("per_other", "ses_other_chat", "rm -rf /"),
            ask_frame("per_other_sub", "ses_other_child", "rm -rf /"),
        ],
        |oc| {
            // Another chat's turn, and a subagent of it.
            oc.set_root_session("ses_other_chat");
            oc.set_subagent_session("ses_other_child", "ses_other_chat", Some("motoko"));
        },
    )
    .await;

    assert!(
        !tg.sent_messages().iter().any(|m| m.text.contains("🔐")),
        "another turn's gates must not prompt here, got {:?}",
        tg.sent_messages()
    );
    assert!(
        db.list_approvals().unwrap().is_empty(),
        "no approval row should be stored for a foreign gate"
    );
}

/// An unknown session (404 — e.g. an id from another opencode instance) is not
/// surfaced, and doesn't take the turn down with it (#88).
#[tokio::test]
async fn unresolvable_session_permission_is_not_surfaced() {
    let (_oc, tg, _db) =
        permission_turn(vec![ask_frame("per_ghost", "ses_ghost", "df")], |_| {}).await;

    assert!(
        !tg.sent_messages().iter().any(|m| m.text.contains("🔐")),
        "an unresolvable gate must not prompt, got {:?}",
        tg.sent_messages()
    );
}

/// The depth-cap walk must not POISON the cache: a chain deeper than the cap is
/// inconclusive, not proven-foreign. If the too-deep walk caches every session it
/// touched as foreign, a later gate from a shallow session on that same chain --
/// which IS a legitimate descendant of this turn -- gets silently swallowed.
///
/// ses_lvl9 -> ... -> ses_lvl1 -> SESSION. A gate from ses_lvl9 is 9 hops (past the
/// 8-hop cap) and must stay silent. A gate from ses_lvl2 is only 2 hops and MUST
/// prompt -- but the too-deep walk passed straight through ses_lvl2 on its way up.
#[tokio::test]
async fn deep_walk_does_not_poison_a_shallow_sibling_on_the_same_chain() {
    let (_oc, tg, _db) = permission_turn(
        vec![
            ask_frame("per_far", "ses_lvl9", "df"),
            ask_frame("per_near", "ses_lvl2", "ls"),
        ],
        |oc| {
            oc.set_subagent_session("ses_lvl1", SESSION, Some("motoko"));
            for lvl in 2..=9 {
                let id = format!("ses_lvl{lvl}");
                let parent = format!("ses_lvl{}", lvl - 1);
                oc.set_subagent_session(&id, &parent, Some("motoko"));
            }
        },
    )
    .await;

    let prompts: Vec<_> = tg
        .sent_messages()
        .into_iter()
        .filter(|m| m.text.contains("🔐"))
        .collect();
    assert_eq!(
        prompts.len(),
        1,
        "the 2-hop gate must still prompt after the 9-hop walk passed through it; got {prompts:?}"
    );
    assert!(
        prompts[0].text.contains("ls"),
        "the surfaced gate should be the shallow one (`ls`), got {prompts:?}"
    );
}
