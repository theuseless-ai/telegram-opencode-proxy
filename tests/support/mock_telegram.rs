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
    /// Count of send/edit attempts rejected for whitespace-only text — the
    /// driver's non-empty guard (#8) should keep this at 0.
    empty_rejections: Arc<AtomicI64>,
    /// Total `SendMessage` + `EditMessageText` attempts (incl. injected
    /// failures) — lets a retry test count how many tries it took (#25).
    send_attempts: Arc<AtomicI64>,
    /// Remaining send/edit calls to answer with `429 retry_after: 0` (flood).
    fail_429: Arc<AtomicI64>,
    /// Remaining send/edit calls to answer with `400 Bad Request`.
    fail_400: Arc<AtomicI64>,
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
    empty_rejections: Arc<AtomicI64>,
    send_attempts: Arc<AtomicI64>,
    fail_429: Arc<AtomicI64>,
    fail_400: Arc<AtomicI64>,
    updates: Arc<Mutex<VecDeque<Value>>>,
}

impl MockTelegram {
    /// Bind an ephemeral port and start serving.
    pub async fn start() -> Self {
        let sent: Arc<Mutex<Vec<SentMessage>>> = Arc::new(Mutex::new(Vec::new()));
        let edits: Arc<Mutex<Vec<EditMessage>>> = Arc::new(Mutex::new(Vec::new()));
        let chat_actions: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let empty_rejections = Arc::new(AtomicI64::new(0));
        let send_attempts = Arc::new(AtomicI64::new(0));
        let fail_429 = Arc::new(AtomicI64::new(0));
        let fail_400 = Arc::new(AtomicI64::new(0));
        let updates: Arc<Mutex<VecDeque<Value>>> = Arc::new(Mutex::new(VecDeque::new()));
        let state = TgState {
            sent: Arc::clone(&sent),
            edits: Arc::clone(&edits),
            chat_actions: Arc::clone(&chat_actions),
            empty_rejections: Arc::clone(&empty_rejections),
            send_attempts: Arc::clone(&send_attempts),
            fail_429: Arc::clone(&fail_429),
            fail_400: Arc::clone(&fail_400),
            updates: Arc::clone(&updates),
            next_msg_id: Arc::new(AtomicI64::new(1)),
            bot_id: 424242,
        };

        let app = Router::new()
            .route("/bot{token}/{method}", any(handler))
            // File download endpoint teloxide hits after `getFile` (#11): serves
            // canned bytes for any path.
            .route("/file/bot{token}/{*path}", any(download_file))
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
            empty_rejections,
            send_attempts,
            fail_429,
            fail_400,
            updates,
        }
    }

    /// Answer the next `n` send/edit calls with `429 retry_after: 0` (flood
    /// control), then behave normally — drives the retry/backoff path (#25).
    #[allow(dead_code)]
    pub fn fail_next_429(&self, n: i64) {
        self.fail_429.store(n, Ordering::SeqCst);
    }

    /// Answer the next `n` send/edit calls with `400 Bad Request` (a
    /// non-transient error the retry path must NOT retry). (#25)
    #[allow(dead_code)]
    pub fn fail_next_400(&self, n: i64) {
        self.fail_400.store(n, Ordering::SeqCst);
    }

    /// Total `sendMessage` + `editMessageText` attempts, including injected
    /// failures — a retry test asserts how many tries a call took.
    #[allow(dead_code)]
    pub fn send_attempts(&self) -> i64 {
        self.send_attempts.load(Ordering::SeqCst)
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

    /// How many send/edit attempts were rejected for whitespace-only text. The
    /// streaming driver's non-empty guard (#8) should keep this at 0.
    #[allow(dead_code)]
    pub fn empty_rejections(&self) -> i64 {
        self.empty_rejections.load(Ordering::SeqCst)
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

    if matches!(method.as_str(), "SendMessage" | "EditMessageText") {
        st.send_attempts.fetch_add(1, Ordering::SeqCst);

        // Injected flood control (#25): 429 + `retry_after: 0` so the retry
        // wrapper backs off (instantly) and tries again.
        if st.fail_429.load(Ordering::SeqCst) > 0 {
            st.fail_429.fetch_sub(1, Ordering::SeqCst);
            return Json(json!({
                "ok": false,
                "error_code": 429,
                "description": "Too Many Requests: retry after 0",
                "parameters": { "retry_after": 0 }
            }));
        }
        // Injected non-transient error (#25): the retry wrapper must NOT retry.
        if st.fail_400.load(Ordering::SeqCst) > 0 {
            st.fail_400.fetch_sub(1, Ordering::SeqCst);
            return Json(json!({
                "ok": false,
                "error_code": 400,
                "description": "Bad Request: something is wrong"
            }));
        }

        // Mirror Telegram: a whitespace-only text body is rejected, so the
        // streaming driver's non-empty guard (#8) is exercised here.
        if req["text"].as_str().unwrap_or_default().trim().is_empty() {
            st.empty_rejections.fetch_add(1, Ordering::SeqCst);
            return Json(json!({
                "ok": false,
                "error_code": 400,
                "description": "Bad Request: text must be non-empty"
            }));
        }
    }

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
        "GetFile" => {
            // Echo the requested file id and point at a downloadable path (#11).
            let file_id = req["file_id"].as_str().unwrap_or("file").to_string();
            json!({
                "file_id": file_id,
                "file_unique_id": "uniq",
                "file_size": MOCK_FILE_BYTES.len(),
                "file_path": "downloads/file.bin"
            })
        }
        // DeleteWebhook and any other method: a bare `true` result is valid.
        _ => Value::Bool(true),
    };

    Json(json!({ "ok": true, "result": result }))
}

/// Canned bytes served by the file-download endpoint (#11).
const MOCK_FILE_BYTES: &[u8] = b"MOCKFILE";

/// `GET /file/bot{token}/{path}` — teloxide's file download; returns fixed bytes.
async fn download_file(Path((_token, _path)): Path<(String, String)>) -> Vec<u8> {
    MOCK_FILE_BYTES.to_vec()
}
