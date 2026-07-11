//! `mock_model` — a tiny OpenAI-compatible server for the **Layer 2** full-stack
//! harness (issue #24). LOCAL-ONLY: it is an `example` (compiled by
//! `--all-targets`, never shipped in the proxy binary) and is not run by CI.
//!
//! It implements just enough of the OpenAI API for a real `opencode serve` to
//! treat it as a provider `baseURL`:
//!
//! - `GET  /v1/models`          — advertise one model id.
//! - `POST /v1/chat/completions` — return a canned, deterministic completion.
//!
//! `test-fullstack.sh` writes an `opencode.json` whose provider `baseURL` points
//! here, starts a real `opencode serve`, then drives the proxy end-to-end.
//!
//! Run standalone: `cargo run --example mock_model -- 127.0.0.1:8088`
//! (defaults to `127.0.0.1:8088`). The model id is `mock-model`.

use std::net::SocketAddr;

use axum::Json;
use axum::routing::{get, post};
use axum::{Router, body::Bytes};
use serde_json::{Value, json};

/// The single model id this mock advertises and answers as.
const MODEL_ID: &str = "mock-model";
/// Deterministic assistant reply for every completion request.
const CANNED_REPLY: &str = "PONG from mock_model";

#[tokio::main]
async fn main() {
    let addr: SocketAddr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:8088".to_string())
        .parse()
        .expect("usage: mock_model [HOST:PORT]");

    let app = Router::new()
        .route("/v1/models", get(models))
        .route("/v1/chat/completions", post(chat_completions));

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .expect("bind mock_model");
    eprintln!("mock_model listening on http://{addr} (model: {MODEL_ID})");
    axum::serve(listener, app).await.expect("serve mock_model");
}

/// `GET /v1/models` — OpenAI model list.
async fn models() -> Json<Value> {
    Json(json!({
        "object": "list",
        "data": [{ "id": MODEL_ID, "object": "model", "owned_by": "mock" }]
    }))
}

/// `POST /v1/chat/completions` — a single, non-streaming canned completion.
async fn chat_completions(_body: Bytes) -> Json<Value> {
    Json(json!({
        "id": "chatcmpl-mock",
        "object": "chat.completion",
        "created": 0,
        "model": MODEL_ID,
        "choices": [{
            "index": 0,
            "message": { "role": "assistant", "content": CANNED_REPLY },
            "finish_reason": "stop"
        }],
        "usage": { "prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0 }
    }))
}
