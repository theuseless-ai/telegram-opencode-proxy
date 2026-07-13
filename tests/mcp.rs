//! Hermetic tests for the #65 MCP file-transfer feature (final download-URL
//! design). No network, no real `opencode`, no real Telegram — the two
//! in-process mocks (`mock_opencode` + `mock_telegram`) are wired to the REAL
//! MCP server ([`mcp::build_router`]) and the REAL inbound-media handler
//! ([`handle_media`]), and exercised through a faithful in-process **rmcp
//! streamable-http client** (outbound) and a plain `reqwest` GET (inbound
//! download).
//!
//! Coverage:
//!
//! - **Outbound** `send_file_to_user`: the `X-Slot` header routes an outbound
//!   file to the slot-owning chat; a document filename arrives as a
//!   `SendDocument`, an image filename as a `SendPhoto`.
//! - **`X-Slot` guard**: a call with no header, or an unknown slot, is a clean
//!   tool error and nothing is sent.
//! - **Cross-slot routing**: two bound slots; a send under one slot's header
//!   never reaches the other slot's chat.
//! - **Inbound announce + download**: `handle_media` stores the file and injects
//!   an announce prompt carrying a one-shot `/files/<uuid>` URL; `GET`ting that
//!   id returns the bytes (200) once, then 404 (single-use); a non-uuid id is
//!   400 and an unknown-but-valid uuid is 404.
//! - **FilePart fallback** is left to `harness.rs::inbound_photo_is_sent_as_a_file_part`
//!   (unchanged) — this file only exercises the default announce path.

// This crate exercises only a subset of the shared mock surface (e.g. it never
// pins a reply or drives the without-model catalogue), so silence dead-code
// lints for the parts of `support` it doesn't touch — the other consumers
// (`harness.rs`, `streaming.rs`) use the rest.
#[allow(dead_code)]
mod support;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use reqwest::header::{HeaderName, HeaderValue};
use serde_json::json;
use teloxide::Bot;
use teloxide::types::Message;
use tokio_util::sync::CancellationToken;

use rmcp::ServiceExt;
use rmcp::model::CallToolRequestParams;
use rmcp::transport::StreamableHttpClientTransport;
use rmcp::transport::streamable_http_client::StreamableHttpClientTransportConfig;

use telegram_opencode_proxy::config::{Config, Mcp, Model, Pairing, Permissions, Slot};
use telegram_opencode_proxy::connect_slots;
use telegram_opencode_proxy::mcp;
use telegram_opencode_proxy::persistence::Db;
use telegram_opencode_proxy::telegram::bot::{AppState, handle_media};

use support::mock_opencode::MockOpencode;
use support::mock_telegram::MockTelegram;

const CHAT_A: i64 = 111;
const CHAT_B: i64 = 222;

// --- config + state builders (mirroring `tests/harness.rs`) -------------------

/// A single-slot config named `slot`, bound to `chat`, pointing at `opencode_url`.
/// `mcp` is the default (enabled, no FilePart fallback) — the announce path.
fn config_for(opencode_url: &str, slot: &str, chat: i64) -> Config {
    Config {
        bot_token: "12345:test-token".into(),
        admin_socket: "/tmp/mock-admin.sock".into(),
        slots: vec![Slot {
            name: slot.to_string(),
            opencode_url: opencode_url.to_string(),
            workdir: ".".into(),
            telegram_id: Some(chat),
        }],
        model: Model {
            provider_id: "llm-lan".to_string(),
            model_id: "Qwen3.6-35B-A3B-bf16".to_string(),
        },
        permissions: Permissions { ask: Vec::new() },
        pairing: Pairing::default(),
        db_path: "proxy.db".into(),
        mcp: Mcp::default(),
    }
}

/// A two-slot config — both slots point at the same mock opencode, each bound to
/// its own chat. Drives the cross-slot routing test (`slot_snapshot` sees both).
fn two_slot_config(opencode_url: &str, a: (&str, i64), b: (&str, i64)) -> Config {
    let mut cfg = config_for(opencode_url, a.0, a.1);
    cfg.slots.push(Slot {
        name: b.0.to_string(),
        opencode_url: opencode_url.to_string(),
        workdir: ".".into(),
        telegram_id: Some(b.1),
    });
    cfg
}

/// A `teloxide::Bot` whose whole API base is the mock — so every request goes to
/// `{mock}/bot{token}/{Method}` and never touches `api.telegram.org`.
fn bot_pointed_at(mock: &MockTelegram) -> Bot {
    Bot::new("12345:test-token").set_api_url(
        mock.url
            .parse()
            .expect("mock_telegram url parses as an API base"),
    )
}

/// Bring up the real per-slot registry for `cfg` against the running mock, and
/// build an [`AppState`] whose `bot` is the mock-pointed handle (so outbound
/// sends are recorded).
async fn state_for(cfg: Config, bot: Bot) -> Arc<AppState> {
    let registry = connect_slots(&cfg)
        .await
        .expect("connect_slots (readiness + model validation) succeeds against the mock");
    let db = Db::open_in_memory().expect("in-memory persistence store opens");
    AppState::new(cfg, "unused.toml".into(), registry, db, bot)
}

/// A running MCP server plus its live listen port.
struct Served {
    state: Arc<AppState>,
    port: u16,
    ct: CancellationToken,
    handle: tokio::task::JoinHandle<()>,
}

impl Served {
    fn base(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }
}

impl Drop for Served {
    fn drop(&mut self) {
        self.ct.cancel();
        self.handle.abort();
    }
}

/// Bind an ephemeral 127.0.0.1 port, stamp it into `cfg.mcp.port` (so the inbound
/// announce URL matches the live port), build the real [`AppState`], and serve
/// [`mcp::build_router`] on it in a spawned task.
async fn serve_mcp(mut cfg: Config, bot: Bot) -> Served {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral MCP listener");
    let port = listener.local_addr().expect("local_addr").port();
    cfg.mcp.port = port;
    cfg.mcp.bind = "127.0.0.1".parse().expect("loopback IP parses");

    let state = state_for(cfg, bot).await;
    let ct = CancellationToken::new();
    let router = mcp::build_router(state.clone(), ct.clone());
    let handle = tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });

    Served {
        state,
        port,
        ct,
        handle,
    }
}

/// An rmcp streamable-http client connected to `base/mcp`, sending the given
/// `X-Slot` header on every request (`None` → no header at all).
async fn mcp_client(
    base: &str,
    slot: Option<&str>,
) -> rmcp::service::RunningService<rmcp::RoleClient, ()> {
    let mut headers: HashMap<HeaderName, HeaderValue> = HashMap::new();
    if let Some(slot) = slot {
        headers.insert(
            HeaderName::from_static("x-slot"),
            HeaderValue::from_str(slot).expect("slot is a valid header value"),
        );
    }
    let config = StreamableHttpClientTransportConfig::with_uri(format!("{base}/mcp"))
        .custom_headers(headers);
    let transport = StreamableHttpClientTransport::from_config(config);
    ().serve(transport)
        .await
        .expect("rmcp client initializes against the stateless server")
}

/// Invoke `send_file_to_user` with `content` already base64-encoded.
async fn call_send_file(
    client: &rmcp::service::RunningService<rmcp::RoleClient, ()>,
    filename: &str,
    bytes: &[u8],
    caption: Option<&str>,
) -> Result<rmcp::model::CallToolResult, rmcp::ServiceError> {
    let mut args = json!({
        "filename": filename,
        "content": STANDARD.encode(bytes),
    });
    if let Some(caption) = caption {
        args["caption"] = json!(caption);
    }
    let params = CallToolRequestParams::new("send_file_to_user")
        .with_arguments(args.as_object().expect("args is an object").clone());
    client.call_tool(params).await
}

/// Poll `pred` every 10ms until it holds or `budget` elapses.
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

// --- message builders ---------------------------------------------------------

/// A private-chat photo `Message` with a caption, from the Bot API wire shape.
fn photo_message(chat_id: i64, caption: &str) -> Message {
    serde_json::from_value(json!({
        "message_id": 1,
        "date": 0,
        "chat": { "id": chat_id, "type": "private", "first_name": "Tester" },
        "from": { "id": chat_id, "is_bot": false, "first_name": "Tester" },
        "photo": [
            { "file_id": "f1", "file_unique_id": "u1", "file_size": 8, "width": 90, "height": 90 }
        ],
        "caption": caption
    }))
    .expect("constructing a photo Message from wire JSON")
}

/// A private-chat document `Message` (a `.txt`), from the Bot API wire shape.
fn document_message(chat_id: i64, filename: &str) -> Message {
    serde_json::from_value(json!({
        "message_id": 1,
        "date": 0,
        "chat": { "id": chat_id, "type": "private", "first_name": "Tester" },
        "from": { "id": chat_id, "is_bot": false, "first_name": "Tester" },
        "document": {
            "file_id": "d1",
            "file_unique_id": "du1",
            "file_size": 8,
            "file_name": filename
        }
    }))
    .expect("constructing a document Message from wire JSON")
}

// --- 1. outbound happy path ---------------------------------------------------

/// `send_file_to_user` with a document filename → the bound chat receives a
/// `SendDocument`; with an image filename → a `SendPhoto`. Both routed by the
/// `X-Slot` header, never a tool argument.
#[tokio::test]
async fn outbound_send_delivers_document_and_photo() {
    let oc = MockOpencode::start().await;
    let tg = MockTelegram::start().await;
    let served = serve_mcp(config_for(&oc.url, "frank", CHAT_A), bot_pointed_at(&tg)).await;
    let client = mcp_client(&served.base(), Some("frank")).await;

    // A document filename → SendDocument.
    call_send_file(&client, "note.txt", b"hello note", Some("a caption"))
        .await
        .expect("send_file_to_user (document) succeeds");
    // An image filename → SendPhoto.
    call_send_file(&client, "pic.png", b"\x89PNG\r\n\x1a\n", None)
        .await
        .expect("send_file_to_user (image) succeeds");

    let files = tg.sent_files();
    let doc = files
        .iter()
        .find(|f| f.filename == "note.txt")
        .expect("the document was delivered");
    assert_eq!(doc.chat_id, CHAT_A, "document routed to the bound chat");
    assert_eq!(doc.kind, "document", "a .txt is a document, not a photo");

    let photo = files
        .iter()
        .find(|f| f.filename == "pic.png")
        .expect("the image was delivered");
    assert_eq!(photo.chat_id, CHAT_A, "photo routed to the bound chat");
    assert_eq!(photo.kind, "photo", "a .png is sent as a photo");

    let _ = client.cancel().await;
}

// --- 2. X-Slot guard ----------------------------------------------------------

/// A `send_file_to_user` with NO `X-Slot` header is a clean tool error and sends
/// nothing.
#[tokio::test]
async fn outbound_without_slot_header_is_an_error() {
    let oc = MockOpencode::start().await;
    let tg = MockTelegram::start().await;
    let served = serve_mcp(config_for(&oc.url, "frank", CHAT_A), bot_pointed_at(&tg)).await;
    let client = mcp_client(&served.base(), None).await;

    let res = call_send_file(&client, "note.txt", b"nope", None).await;
    assert!(res.is_err(), "a missing X-Slot header must be a tool error");
    assert!(
        tg.sent_files().is_empty(),
        "nothing must be sent without a validated slot, got {:?}",
        tg.sent_files()
    );

    let _ = client.cancel().await;
}

/// A `send_file_to_user` whose `X-Slot` names a slot NOT in the live registry is
/// a clean tool error and sends nothing.
#[tokio::test]
async fn outbound_with_unknown_slot_is_an_error() {
    let oc = MockOpencode::start().await;
    let tg = MockTelegram::start().await;
    let served = serve_mcp(config_for(&oc.url, "frank", CHAT_A), bot_pointed_at(&tg)).await;
    let client = mcp_client(&served.base(), Some("ghost")).await;

    let res = call_send_file(&client, "note.txt", b"nope", None).await;
    assert!(res.is_err(), "an unknown slot must be a tool error");
    assert!(
        tg.sent_files().is_empty(),
        "an unknown slot must send nothing, got {:?}",
        tg.sent_files()
    );

    let _ = client.cancel().await;
}

// --- 3. cross-slot routing ----------------------------------------------------

/// Two bound slots share one server; a send under `frank`'s header lands in
/// `frank`'s chat, and a send under `holly`'s header lands in `holly`'s chat —
/// never crossed.
#[tokio::test]
async fn outbound_routes_per_slot_never_crosses() {
    let oc = MockOpencode::start().await;
    let tg = MockTelegram::start().await;
    let cfg = two_slot_config(&oc.url, ("frank", CHAT_A), ("holly", CHAT_B));
    let served = serve_mcp(cfg, bot_pointed_at(&tg)).await;

    let holly = mcp_client(&served.base(), Some("holly")).await;
    call_send_file(&holly, "holly.txt", b"for holly", None)
        .await
        .expect("holly's send succeeds");

    let frank = mcp_client(&served.base(), Some("frank")).await;
    call_send_file(&frank, "frank.txt", b"for frank", None)
        .await
        .expect("frank's send succeeds");

    let files = tg.sent_files();
    let holly_file = files
        .iter()
        .find(|f| f.filename == "holly.txt")
        .expect("holly's file delivered");
    assert_eq!(holly_file.chat_id, CHAT_B, "holly's file goes to holly");
    let frank_file = files
        .iter()
        .find(|f| f.filename == "frank.txt")
        .expect("frank's file delivered");
    assert_eq!(frank_file.chat_id, CHAT_A, "frank's file goes to frank");

    // No file ever landed in the wrong chat.
    assert!(
        !files
            .iter()
            .any(|f| f.filename == "holly.txt" && f.chat_id == CHAT_A),
        "holly's file must never reach frank's chat"
    );

    let _ = holly.cancel().await;
    let _ = frank.cancel().await;
}

// --- 4. inbound announce + download ------------------------------------------

/// Pull the first `/files/<uuid>` URL out of a prompt/announce string.
fn extract_download_id(text: &str) -> String {
    let marker = "/files/";
    let start = text.find(marker).expect("announce carries a /files/ URL") + marker.len();
    let rest = &text[start..];
    let end = rest
        .find(|c: char| !(c.is_ascii_hexdigit() || c == '-'))
        .unwrap_or(rest.len());
    rest[..end].to_string()
}

/// A document announce carries a one-shot download URL; `GET`ting it returns the
/// bytes once, then 404 (single-use). The announce wording tells the model to
/// EXTRACT the text.
#[tokio::test]
async fn inbound_document_announce_then_download_is_single_use() {
    let oc = MockOpencode::start().await;
    let tg = MockTelegram::start().await;
    let served = serve_mcp(config_for(&oc.url, "frank", CHAT_A), bot_pointed_at(&tg)).await;

    handle_media(
        bot_pointed_at(&tg),
        document_message(CHAT_A, "note.txt"),
        served.state.clone(),
    )
    .await
    .expect("handle_media (document) succeeds");

    // The mock opencode echoes the prompt back as `echo: <announce>`; await it.
    let got = wait_for(
        || {
            tg.sent_messages()
                .iter()
                .any(|m| m.text.contains("/files/"))
        },
        Duration::from_secs(5),
    )
    .await;
    assert!(
        got,
        "an announce with a download URL must arrive, got {:?}",
        tg.sent_messages()
    );

    let announce = tg
        .sent_messages()
        .into_iter()
        .map(|m| m.text)
        .find(|t| t.contains("/files/"))
        .expect("announce present");
    assert!(
        announce.contains("EXTRACT"),
        "a document announce must instruct the model to EXTRACT the text, got: {announce}"
    );
    assert!(announce.contains("note.txt"), "the announce names the file");

    let id = extract_download_id(&announce);
    let http = reqwest::Client::new();

    // First GET → 200 with the file bytes + a Content-Type.
    let resp = http
        .get(format!("{}/files/{}", served.base(), id))
        .send()
        .await
        .expect("download request sends");
    assert_eq!(resp.status(), 200, "first download of a fresh id is 200");
    assert!(
        resp.headers().get(reqwest::header::CONTENT_TYPE).is_some(),
        "the download carries a Content-Type"
    );
    let body = resp.bytes().await.expect("read body");
    assert_eq!(
        &body[..],
        b"MOCKFILE",
        "the download returns the stored file bytes"
    );

    // Second GET of the same id → 404 (single-use consumption).
    let resp2 = http
        .get(format!("{}/files/{}", served.base(), id))
        .send()
        .await
        .expect("second download request sends");
    assert_eq!(resp2.status(), 404, "a consumed id is 404 (single-use)");
}

/// An image announce tells the model to VIEW the image; the download endpoint's
/// error surface: a non-uuid id → 400, an unknown-but-valid uuid → 404.
#[tokio::test]
async fn inbound_image_announce_and_download_error_surface() {
    let oc = MockOpencode::start().await;
    let tg = MockTelegram::start().await;
    let served = serve_mcp(config_for(&oc.url, "frank", CHAT_A), bot_pointed_at(&tg)).await;

    handle_media(
        bot_pointed_at(&tg),
        photo_message(CHAT_A, "what is this?"),
        served.state.clone(),
    )
    .await
    .expect("handle_media (photo) succeeds");

    let got = wait_for(
        || {
            tg.sent_messages()
                .iter()
                .any(|m| m.text.contains("/files/"))
        },
        Duration::from_secs(5),
    )
    .await;
    assert!(
        got,
        "an image announce must arrive, got {:?}",
        tg.sent_messages()
    );

    let announce = tg
        .sent_messages()
        .into_iter()
        .map(|m| m.text)
        .find(|t| t.contains("/files/"))
        .expect("announce present");
    assert!(
        announce.contains("VIEW"),
        "an image announce must instruct the model to VIEW the image, got: {announce}"
    );

    let http = reqwest::Client::new();

    // A non-uuid id never reaches the store → 400.
    let bad = http
        .get(format!("{}/files/not-a-uuid", served.base()))
        .send()
        .await
        .expect("bad-id request sends");
    assert_eq!(bad.status(), 400, "a non-uuid id is a 400");

    // A syntactically valid but unknown uuid → 404.
    let unknown = http
        .get(format!(
            "{}/files/00000000-0000-4000-8000-000000000000",
            served.base()
        ))
        .send()
        .await
        .expect("unknown-id request sends");
    assert_eq!(unknown.status(), 404, "an unknown uuid is a 404");
}
