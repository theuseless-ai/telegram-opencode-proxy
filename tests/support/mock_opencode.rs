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

use axum::Json;
use axum::extract::{Path, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Router, body::Bytes};
use serde_json::{Value, json};

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

    async fn start_inner(include_model: bool, config_fails: u64) -> Self {
        let reply: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
        let event_body: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
        let event_connections = Arc::new(AtomicU64::new(0));
        let message_list: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
        let state = OcState {
            include_model,
            reply: Arc::clone(&reply),
            sessions: Arc::new(Mutex::new(HashSet::new())),
            next_id: Arc::new(AtomicU64::new(1)),
            config_fails: Arc::new(AtomicU64::new(config_fails)),
            event_body: Arc::clone(&event_body),
            event_connections: Arc::clone(&event_connections),
            message_list: Arc::clone(&message_list),
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
        }
    }
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
    let req: Value = serde_json::from_slice(&body).unwrap_or_else(|_| json!({}));
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

/// `GET /global/event` — SSE stub. Serves the [`set_event_stream`] body (or a
/// minimal `server.connected`-only frame), then the response ends and the
/// connection closes, so a reconnecting client re-fetches it. Each accepted
/// connection bumps [`event_connections`], so a test can assert the reconnect.
///
/// [`set_event_stream`]: MockOpencode::set_event_stream
/// [`event_connections`]: MockOpencode::event_connections
async fn global_event(State(st): State<OcState>) -> Response {
    st.event_connections.fetch_add(1, Ordering::SeqCst);
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
