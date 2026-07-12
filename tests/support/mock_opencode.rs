//! `mock_opencode` — an in-process HTTP mock of the opencode-serve endpoints the
//! proxy calls, backed by canned/fixture-shaped data (issue #24, Layer 1).
//!
//! It implements just the surface `OpencodeClient` / `health::wait_ready` /
//! `session::get_or_create` touch, wired against the **A0-validated** V1 wire
//! (`architecture.md` §10):
//!
//! | Method & path                 | Behaviour                                          |
//! |-------------------------------|----------------------------------------------------|
//! | `GET  /config`                | `200 {}` — readiness probe.                        |
//! | `GET  /config/providers`      | provider catalogue; two variants (see below).      |
//! | `POST /session`               | mints `ses_mock_<n>`, records it, returns it.      |
//! | `GET  /session/:id`           | `200` if the id was minted here, else `404`.       |
//! | `PATCH /session/:id`          | `200 {}` — the deny-posture PATCH.                 |
//! | `POST /session/:id/message`   | canned completed [`MessageEnvelope`]; echoes the   |
//! |                               | prompt, or a fixed reply set via [`set_reply`].    |
//! | `GET  /session/:id/message`   | canned assistant message list (backfill, #7).      |
//! | `GET  /global/event`          | replays a canned SSE body, then closes so the      |
//! |                               | client reconnects; connection count is observable. |
//!
//! The `/config/providers` catalogue has two variants so tests can exercise both
//! model-validation outcomes: [`MockOpencode::start`] returns a catalogue that
//! **contains** the model the harness configures (`llm-lan` /
//! `Qwen3.6-35B-A3B-bf16`, so `validate_model` passes), while
//! [`MockOpencode::start_without_model`] returns a catalogue that does **not**.
//!
//! [`MessageEnvelope`]: telegram_opencode_proxy::opencode::types::MessageEnvelope
//! [`set_reply`]: MockOpencode::set_reply

use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use std::time::Duration;

use axum::Json;
use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Router, body::Bytes};
use futures_util::StreamExt;
use serde_json::{Value, json};

/// Paced SSE frames `(frames, gap_ms)` for `GET /global/event` (#8 streaming).
type EventFrames = Arc<Mutex<Option<(Vec<String>, u64)>>>;

/// Shared mutable state behind the mock's handlers.
#[derive(Clone)]
struct OcState {
    /// Whether `/config/providers` advertises the harness-configured model.
    include_model: bool,
    /// Optional fixed reply for `POST /session/:id/message`; `None` echoes.
    reply: Arc<Mutex<Option<String>>>,
    /// Session ids minted by `POST /session` (so `GET /session/:id` can 404).
    sessions: Arc<Mutex<HashSet<String>>>,
    /// Monotonic counter for minted session ids.
    next_id: Arc<AtomicU64>,
    /// Number of remaining `GET /config` requests that must answer `503`
    /// (simulating a down instance) before readiness starts succeeding. Lets a
    /// test drive the `connect` reconnect path: the first probe sees the slot
    /// down, the reconnect bring-up then finds it up.
    config_fails: Arc<AtomicU64>,
    /// SSE body served by `GET /global/event`; `None` → the minimal
    /// `server.connected`-only stub. Set via [`MockOpencode::set_event_stream`].
    event_body: Arc<Mutex<Option<String>>>,
    /// Count of `GET /global/event` connections accepted — a reconnect bumps
    /// this, so a test can assert the client re-subscribed.
    event_connections: Arc<AtomicU64>,
    /// Canned body for `GET /session/:id/message` (backfill, #7); `None` → `[]`.
    message_list: Arc<Mutex<Option<String>>>,
    /// Paced SSE frames `(frames, gap_ms)` served by `GET /global/event` — each
    /// frame is flushed after `gap_ms`, so the stream trickles like a live turn
    /// (#8). Takes precedence over `event_body` when set.
    event_frames: EventFrames,
    /// Delay (ms) before `POST /session/:id/message` returns, so the paced SSE
    /// stream and its throttled edits happen before the blocking turn resolves.
    message_delay_ms: Arc<AtomicU64>,
    /// Session ids passed to `POST /session/:id/abort` (the `/stop` path, #9).
    aborts: Arc<Mutex<Vec<String>>>,
    /// MIME types of `type:"file"` parts seen on `POST /session/:id/message`
    /// (inbound files, #11).
    file_part_mimes: Arc<Mutex<Vec<String>>>,
    /// `(permission_id, reply)` from `POST /permission/:id/reply` (the relay, #13).
    permission_replies: Arc<Mutex<Vec<(String, String)>>>,
}

/// A running in-process mock opencode instance. Dropping it leaves the spawned
/// server task detached; it stops when the test's tokio runtime is torn down.
pub struct MockOpencode {
    /// Base URL to point `OpencodeClient` / `Slot.opencode_url` at.
    pub url: String,
    reply: Arc<Mutex<Option<String>>>,
    event_body: Arc<Mutex<Option<String>>>,
    event_connections: Arc<AtomicU64>,
    message_list: Arc<Mutex<Option<String>>>,
    event_frames: EventFrames,
    message_delay_ms: Arc<AtomicU64>,
    aborts: Arc<Mutex<Vec<String>>>,
    file_part_mimes: Arc<Mutex<Vec<String>>>,
    permission_replies: Arc<Mutex<Vec<(String, String)>>>,
}

impl MockOpencode {
    /// Start a mock whose provider catalogue **contains** the harness model.
    pub async fn start() -> Self {
        Self::start_inner(true, 0).await
    }

    /// Start a mock whose provider catalogue does **not** contain the model, so
    /// `validate_model` fails (the provider-validation-failure test).
    pub async fn start_without_model() -> Self {
        Self::start_inner(false, 0).await
    }

    /// Start a mock whose first `config_fails` readiness probes (`GET /config`)
    /// answer `503` before it starts reporting ready. Drives the `connect`
    /// reconnect path (down at probe, up at reconnect bring-up). Only the admin
    /// test crate uses this; the harness crate does not, hence `allow(dead_code)`.
    #[allow(dead_code)]
    pub async fn start_with_config_failures(config_fails: u64) -> Self {
        Self::start_inner(true, config_fails).await
    }

    /// Pin the reply returned by `POST /session/:id/message` (e.g. a >4096-char
    /// string to exercise chunking). When unset, the mock echoes the prompt.
    pub fn set_reply(&self, text: impl Into<String>) {
        *self.reply.lock().expect("mock_opencode reply lock") = Some(text.into());
    }

    /// Pin the SSE body served by `GET /global/event`. Each connection replays
    /// this whole body then closes, so a reconnecting client re-fetches it.
    /// `body` should be raw SSE (`data: {…}\n\n` frames). Only #7's event tests
    /// use this, hence `allow(dead_code)` for the harness crate.
    #[allow(dead_code)]
    pub fn set_event_stream(&self, body: impl Into<String>) {
        *self.event_body.lock().expect("mock_opencode event lock") = Some(body.into());
    }

    /// Pin the JSON body served by `GET /session/:id/message` (a message-list
    /// array) for the backfill path. Only #7's event tests use this.
    #[allow(dead_code)]
    pub fn set_message_list(&self, json_body: impl Into<String>) {
        *self
            .message_list
            .lock()
            .expect("mock_opencode msglist lock") = Some(json_body.into());
    }

    /// How many `GET /global/event` connections have been accepted so far. A
    /// value ≥ 2 proves the client reconnected. Only #7's event tests use this.
    #[allow(dead_code)]
    pub fn event_connections(&self) -> u64 {
        self.event_connections.load(Ordering::SeqCst)
    }

    /// Serve `frames` on `GET /global/event`, flushing one every `gap`, so the
    /// SSE stream trickles like a live turn (#8 streaming render). Each frame
    /// should be a complete `data: {…}\n\n` block.
    #[allow(dead_code)]
    pub fn set_event_frames(&self, frames: Vec<String>, gap: Duration) {
        *self.event_frames.lock().expect("mock_opencode frames lock") =
            Some((frames, gap.as_millis() as u64));
    }

    /// Delay every `POST /session/:id/message` by `delay` before it returns, so
    /// a paced event stream and its throttled edits complete first (#8).
    #[allow(dead_code)]
    pub fn set_message_delay(&self, delay: Duration) {
        self.message_delay_ms
            .store(delay.as_millis() as u64, Ordering::SeqCst);
    }

    /// Session ids the proxy has aborted via `POST /session/:id/abort` (the
    /// `/stop` path, #9), in order.
    #[allow(dead_code)]
    pub fn aborted_sessions(&self) -> Vec<String> {
        self.aborts
            .lock()
            .expect("mock_opencode aborts lock")
            .clone()
    }

    /// MIME types of file parts the proxy sent in prompts (inbound files, #11).
    #[allow(dead_code)]
    pub fn file_part_mimes(&self) -> Vec<String> {
        self.file_part_mimes
            .lock()
            .expect("mock_opencode file mimes lock")
            .clone()
    }

    /// `(permission_id, reply)` pairs sent to `POST /permission/:id/reply` (#13).
    #[allow(dead_code)]
    pub fn permission_replies(&self) -> Vec<(String, String)> {
        self.permission_replies
            .lock()
            .expect("mock_opencode permission lock")
            .clone()
    }

    async fn start_inner(include_model: bool, config_fails: u64) -> Self {
        let reply: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
        let event_body: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
        let event_connections = Arc::new(AtomicU64::new(0));
        let message_list: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
        let event_frames: EventFrames = Arc::new(Mutex::new(None));
        let message_delay_ms = Arc::new(AtomicU64::new(0));
        let aborts: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let file_part_mimes: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let permission_replies: Arc<Mutex<Vec<(String, String)>>> =
            Arc::new(Mutex::new(Vec::new()));
        let state = OcState {
            include_model,
            reply: Arc::clone(&reply),
            sessions: Arc::new(Mutex::new(HashSet::new())),
            next_id: Arc::new(AtomicU64::new(1)),
            config_fails: Arc::new(AtomicU64::new(config_fails)),
            event_body: Arc::clone(&event_body),
            event_connections: Arc::clone(&event_connections),
            message_list: Arc::clone(&message_list),
            event_frames: Arc::clone(&event_frames),
            message_delay_ms: Arc::clone(&message_delay_ms),
            aborts: Arc::clone(&aborts),
            file_part_mimes: Arc::clone(&file_part_mimes),
            permission_replies: Arc::clone(&permission_replies),
        };

        let app = Router::new()
            .route("/config", get(config))
            .route("/config/providers", get(config_providers))
            .route("/session", post(create_session))
            .route("/session/{id}", get(get_session).patch(patch_session))
            .route(
                "/session/{id}/message",
                post(message).get(get_session_messages),
            )
            .route("/session/{id}/abort", post(abort_session))
            .route("/permission/{id}/reply", post(reply_permission))
            .route("/global/event", get(global_event))
            .with_state(state);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock_opencode listener");
        let addr = listener.local_addr().expect("mock_opencode local_addr");
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        Self {
            url: format!("http://{addr}"),
            reply,
            event_body,
            event_connections,
            message_list,
            event_frames,
            message_delay_ms,
            aborts,
            file_part_mimes,
            permission_replies,
        }
    }
}

/// `POST /session/:id/abort` — record the aborted session id and return `true`,
/// matching the A0 wire (`{200: boolean}`, no body). Drives the `/stop` path (#9).
async fn abort_session(State(st): State<OcState>, Path(id): Path<String>) -> impl IntoResponse {
    st.aborts
        .lock()
        .expect("mock_opencode aborts lock")
        .push(id);
    Json(json!(true))
}

/// `POST /permission/:id/reply` — record `(permission_id, reply)` and return
/// `true`. Drives the permission relay (#13).
async fn reply_permission(
    State(st): State<OcState>,
    Path(id): Path<String>,
    body: Bytes,
) -> impl IntoResponse {
    let req: Value = serde_json::from_slice(&body).unwrap_or_else(|_| json!({}));
    let reply = req["reply"].as_str().unwrap_or_default().to_string();
    st.permission_replies
        .lock()
        .expect("mock_opencode permission lock")
        .push((id, reply));
    Json(json!(true))
}

/// `GET /config` — readiness probe. Answers `503` for the first `config_fails`
/// requests (simulating a down instance), then `200` thereafter.
async fn config(State(st): State<OcState>) -> Response {
    if st.config_fails.load(Ordering::SeqCst) > 0 {
        st.config_fails.fetch_sub(1, Ordering::SeqCst);
        return (StatusCode::SERVICE_UNAVAILABLE, Json(json!({}))).into_response();
    }
    Json(json!({})).into_response()
}

/// `GET /config/providers` — the catalogue `validate_model` checks against.
async fn config_providers(State(st): State<OcState>) -> impl IntoResponse {
    let providers = if st.include_model {
        json!([{
            "id": "llm-lan",
            "name": "Local LLM (mock)",
            "models": { "Qwen3.6-35B-A3B-bf16": { "id": "Qwen3.6-35B-A3B-bf16" } }
        }])
    } else {
        // Provider present, but the configured model is absent — this drives
        // `validate_model`'s missing-model branch, whose error names the model.
        json!([{
            "id": "llm-lan",
            "name": "Local LLM (mock)",
            "models": { "some-other-model": {} }
        }])
    };
    Json(json!({ "providers": providers, "default": {} }))
}

/// `POST /session` — mint and record a session id.
async fn create_session(State(st): State<OcState>, _body: Bytes) -> impl IntoResponse {
    let n = st.next_id.fetch_add(1, Ordering::SeqCst);
    let id = format!("ses_mock_{n}");
    st.sessions
        .lock()
        .expect("mock_opencode sessions lock")
        .insert(id.clone());
    Json(json!({ "id": id, "title": null, "version": "1.17.18" }))
}

/// `GET /session/:id` — `200` for a known id, `404` otherwise (drives recreate).
async fn get_session(State(st): State<OcState>, Path(id): Path<String>) -> Response {
    if st
        .sessions
        .lock()
        .expect("mock_opencode sessions lock")
        .contains(&id)
    {
        (StatusCode::OK, Json(json!({ "id": id }))).into_response()
    } else {
        (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": "unknown session" })),
        )
            .into_response()
    }
}

/// `PATCH /session/:id` — accept the permission-ruleset patch.
async fn patch_session(Path(_id): Path<String>, _body: Bytes) -> impl IntoResponse {
    Json(json!({}))
}

/// `POST /session/:id/message` — blocking turn: return a completed envelope
/// whose text is the pinned reply, else `echo: <prompt>`.
async fn message(
    State(st): State<OcState>,
    Path(id): Path<String>,
    body: Bytes,
) -> impl IntoResponse {
    // Hold the blocking turn open so a paced event stream (and its throttled
    // edits) can play out first — mirrors a real turn where deltas precede the
    // completed message (#8).
    let delay = st.message_delay_ms.load(Ordering::SeqCst);
    if delay > 0 {
        tokio::time::sleep(Duration::from_millis(delay)).await;
    }

    let req: Value = serde_json::from_slice(&body).unwrap_or_else(|_| json!({}));
    // Record any inbound file parts (#11) so a test can assert the proxy attached
    // the download as a base64 data-URI FilePart.
    if let Some(parts) = req["parts"].as_array() {
        let mut mimes = st
            .file_part_mimes
            .lock()
            .expect("mock_opencode file mimes lock");
        for part in parts {
            if part["type"] == "file" {
                mimes.push(part["mime"].as_str().unwrap_or_default().to_string());
            }
        }
    }
    let prompt = req["parts"]
        .as_array()
        .and_then(|parts| parts.iter().find_map(|p| p["text"].as_str()))
        .unwrap_or("");
    let reply = st
        .reply
        .lock()
        .expect("mock_opencode reply lock")
        .clone()
        .unwrap_or_else(|| format!("echo: {prompt}"));

    Json(json!({
        "info": { "id": "msg_mock", "sessionID": id, "role": "assistant", "finish": "stop" },
        "parts": [{ "type": "text", "text": reply }]
    }))
}

/// `GET /global/event` — SSE stub. With [`set_event_frames`] it trickles frames
/// one per gap (a live-turn stream, #8); otherwise it serves the
/// [`set_event_stream`] body (or a minimal `server.connected` frame) in one shot.
/// Either way the response then ends and the connection closes, so a reconnecting
/// client re-fetches it. Each accepted connection bumps [`event_connections`].
///
/// [`set_event_frames`]: MockOpencode::set_event_frames
/// [`set_event_stream`]: MockOpencode::set_event_stream
/// [`event_connections`]: MockOpencode::event_connections
async fn global_event(State(st): State<OcState>) -> Response {
    st.event_connections.fetch_add(1, Ordering::SeqCst);

    // Paced stream: yield one frame per `gap` so the client renders incrementally.
    if let Some((frames, gap_ms)) = st
        .event_frames
        .lock()
        .expect("mock_opencode frames lock")
        .clone()
    {
        let stream = futures_util::stream::iter(frames).then(move |frame| async move {
            if gap_ms > 0 {
                tokio::time::sleep(Duration::from_millis(gap_ms)).await;
            }
            Ok::<Bytes, std::io::Error>(Bytes::from(frame))
        });
        return (
            [(header::CONTENT_TYPE, "text/event-stream")],
            Body::from_stream(stream),
        )
            .into_response();
    }

    let body = st
        .event_body
        .lock()
        .expect("mock_opencode event lock")
        .clone()
        .unwrap_or_else(|| {
            "data: {\"id\":\"evt_mock\",\"type\":\"server.connected\",\"properties\":{}}\n\n"
                .to_string()
        });
    ([(header::CONTENT_TYPE, "text/event-stream")], body).into_response()
}

/// `GET /session/:id/message` — the message list the backfill path reads. Serves
/// the [`set_message_list`](MockOpencode::set_message_list) body, or `[]`.
async fn get_session_messages(State(st): State<OcState>, Path(_id): Path<String>) -> Response {
    let body = st
        .message_list
        .lock()
        .expect("mock_opencode msglist lock")
        .clone()
        .unwrap_or_else(|| "[]".to_string());
    ([(header::CONTENT_TYPE, "application/json")], body).into_response()
}
