//! `mock_telegram` — an in-process HTTP mock of the Telegram Bot API, just wide
//! enough for teloxide 0.17 to talk to it via a custom API URL (issue #24).
//!
//! ## How teloxide is pointed at the mock
//!
//! teloxide (teloxide-core 0.13) builds every request URL as
//! `{base}/bot{token}/{Method}` — see `teloxide_core::net::method_url` — where
//! `{Method}` is the payload's `Payload::NAME`, which the `impl_payload!` macro
//! sets to `stringify!($Method)`, i.e. **PascalCase** (`GetMe`, `SendMessage`,
//! `GetUpdates`, …). Every request is an HTTP `POST` with a JSON body, and every
//! response must be the envelope `{"ok": true, "result": <R>}` (the untagged
//! `TelegramResponse<R>` in `teloxide_core::net::telegram_response`).
//!
//! A test swaps the base with `Bot::new(token).set_api_url(mock.url.parse()?)`
//! (`Bot::set_api_url(reqwest::Url)`), so the whole `{base}` prefix — including
//! the `/bot{token}` segment — is served here. The catch-all route
//! `/bot{token}/{method}` therefore accepts any token.
//!
//! ## Implemented methods
//!
//! - `GetMe` → a canned [`Me`] (bot user).
//! - `GetWebhookInfo` → `{"url": null, …}` so the poller never calls `DeleteWebhook`.
//! - `DeleteWebhook` → `true`.
//! - `GetUpdates` → drains test-injected updates once, then `[]`.
//! - `SendMessage` → **records** `{chat_id, text}` (assertable via
//!   [`MockTelegram::sent_messages`]) and returns a valid `Message`.
//! - `EditMessageText` → **records** `{chat_id, message_id, text}` (assertable
//!   via [`MockTelegram::edits`]) and returns the edited `Message` — the live
//!   streaming path (#8).
//! - `SendChatAction` → **records** the action string (assertable via
//!   [`MockTelegram::chat_actions`]) and returns `true` — the `typing` liveness.
//! - anything else → `true` (harmless default).
//!
//! [`Me`]: https://core.telegram.org/bots/api#getme

use std::collections::VecDeque;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};

use axum::Json;
use axum::extract::{Path, State};
use axum::routing::any;
use axum::{Router, body::Bytes};
use serde_json::{Value, json};

/// A `sendMessage` call the mock recorded.
#[derive(Clone, Debug)]
pub struct SentMessage {
    pub chat_id: i64,
    pub text: String,
}

/// An `editMessageText` call the mock recorded (the live streaming edits, #8).
/// Read by the streaming test crate; `allow(dead_code)` covers crates that only
/// assert on `sent_messages`.
#[allow(dead_code)]
#[derive(Clone, Debug)]
pub struct EditMessage {
    pub chat_id: i64,
    pub message_id: i64,
    pub text: String,
}

/// Shared mutable state behind the mock's single catch-all handler.
#[derive(Clone)]
struct TgState {
    sent: Arc<Mutex<Vec<SentMessage>>>,
    edits: Arc<Mutex<Vec<EditMessage>>>,
    chat_actions: Arc<Mutex<Vec<String>>>,
    updates: Arc<Mutex<VecDeque<Value>>>,
    next_msg_id: Arc<AtomicI64>,
    bot_id: i64,
}

/// A running in-process mock Telegram Bot API. Drop detaches the server task.
pub struct MockTelegram {
    /// Base API URL to hand to `Bot::set_api_url`.
    pub url: String,
    sent: Arc<Mutex<Vec<SentMessage>>>,
    edits: Arc<Mutex<Vec<EditMessage>>>,
    chat_actions: Arc<Mutex<Vec<String>>>,
    updates: Arc<Mutex<VecDeque<Value>>>,
}

impl MockTelegram {
    /// Bind an ephemeral port and start serving.
    pub async fn start() -> Self {
        let sent: Arc<Mutex<Vec<SentMessage>>> = Arc::new(Mutex::new(Vec::new()));
        let edits: Arc<Mutex<Vec<EditMessage>>> = Arc::new(Mutex::new(Vec::new()));
        let chat_actions: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let updates: Arc<Mutex<VecDeque<Value>>> = Arc::new(Mutex::new(VecDeque::new()));
        let state = TgState {
            sent: Arc::clone(&sent),
            edits: Arc::clone(&edits),
            chat_actions: Arc::clone(&chat_actions),
            updates: Arc::clone(&updates),
            next_msg_id: Arc::new(AtomicI64::new(1)),
            bot_id: 424242,
        };

        let app = Router::new()
            .route("/bot{token}/{method}", any(handler))
            .with_state(state);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock_telegram listener");
        let addr = listener.local_addr().expect("mock_telegram local_addr");
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        Self {
            url: format!("http://{addr}"),
            sent,
            edits,
            chat_actions,
            updates,
        }
    }

    /// Snapshot of every recorded `sendMessage` call, in order.
    pub fn sent_messages(&self) -> Vec<SentMessage> {
        self.sent.lock().expect("mock_telegram sent lock").clone()
    }

    /// Snapshot of every recorded `editMessageText` call, in order (#8 streaming).
    #[allow(dead_code)]
    pub fn edits(&self) -> Vec<EditMessage> {
        self.edits.lock().expect("mock_telegram edits lock").clone()
    }

    /// Snapshot of every recorded `sendChatAction` action string (#8 typing).
    #[allow(dead_code)]
    pub fn chat_actions(&self) -> Vec<String> {
        self.chat_actions
            .lock()
            .expect("mock_telegram chat_actions lock")
            .clone()
    }

    /// Queue an incoming `Update` JSON to be served by the next `getUpdates`.
    /// (Used by a dispatcher-driven test / Layer 2; the direct-handler tests do
    /// not need it.)
    #[allow(dead_code)]
    pub fn inject_update(&self, update: Value) {
        self.updates
            .lock()
            .expect("mock_telegram updates lock")
            .push_back(update);
    }
}

/// Single catch-all handler dispatching on the PascalCase `{method}` segment.
async fn handler(
    Path((_token, method)): Path<(String, String)>,
    State(st): State<TgState>,
    body: Bytes,
) -> Json<Value> {
    let req: Value = serde_json::from_slice(&body).unwrap_or_else(|_| json!({}));

    let result = match method.as_str() {
        "GetMe" => json!({
            "id": st.bot_id,
            "is_bot": true,
            "first_name": "MockBot",
            "username": "mock_bot",
            "can_join_groups": true,
            "can_read_all_group_messages": false,
            "supports_inline_queries": false,
            "has_main_web_app": false
        }),
        "GetWebhookInfo" => json!({
            "url": null,
            "has_custom_certificate": false,
            "pending_update_count": 0
        }),
        "GetUpdates" => {
            let mut q = st.updates.lock().expect("mock_telegram updates lock");
            let drained: Vec<Value> = q.drain(..).collect();
            Value::Array(drained)
        }
        "SendMessage" => {
            let chat_id = req["chat_id"].as_i64().unwrap_or_default();
            let text = req["text"].as_str().unwrap_or_default().to_string();
            st.sent
                .lock()
                .expect("mock_telegram sent lock")
                .push(SentMessage {
                    chat_id,
                    text: text.clone(),
                });
            let mid = st.next_msg_id.fetch_add(1, Ordering::SeqCst);
            json!({
                "message_id": mid,
                "date": 0,
                "chat": { "id": chat_id, "type": "private", "first_name": "Test" },
                "from": { "id": st.bot_id, "is_bot": true, "first_name": "MockBot" },
                "text": text
            })
        }
        "EditMessageText" => {
            let chat_id = req["chat_id"].as_i64().unwrap_or_default();
            let message_id = req["message_id"].as_i64().unwrap_or_default();
            let text = req["text"].as_str().unwrap_or_default().to_string();
            st.edits
                .lock()
                .expect("mock_telegram edits lock")
                .push(EditMessage {
                    chat_id,
                    message_id,
                    text: text.clone(),
                });
            json!({
                "message_id": message_id,
                "date": 0,
                "chat": { "id": chat_id, "type": "private", "first_name": "Test" },
                "from": { "id": st.bot_id, "is_bot": true, "first_name": "MockBot" },
                "text": text
            })
        }
        "SendChatAction" => {
            let action = req["action"].as_str().unwrap_or_default().to_string();
            st.chat_actions
                .lock()
                .expect("mock_telegram chat_actions lock")
                .push(action);
            Value::Bool(true)
        }
        // DeleteWebhook and any other method: a bare `true` result is valid.
        _ => Value::Bool(true),
    };

    Json(json!({ "ok": true, "result": result }))
}
