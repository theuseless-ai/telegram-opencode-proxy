//! MCP file-transfer server (issue #65): the proxy hosts **one stateless**
//! `type:"remote"` MCP server that gives the opencode agent two self-describing
//! tools — `send_file_to_user` and `fetch_user_file` — so moving a file is a
//! tool call, not a filesystem convention it must be taught. See
//! `docs/design/architecture.md` §11.
//!
//! # Topology — one shared server, per-request slot
//!
//! A single [`FilesMcp`] is mounted once at `/mcp` and serves **every** slot; it
//! bakes in **no** slot. The caller's identity arrives as the per-request
//! `X-Slot` HTTP header — set by opencode's `opencode.json` config, never by the
//! model (anti-spoofing) — and rmcp forwards the original request
//! [`Parts`](axum::http::request::Parts) to each tool via an
//! [`Extension`](rmcp::handler::server::tool::Extension) extractor. [`FilesMcp::slot_of`]
//! reads that header and **validates it against the live slot registry**
//! ([`AppState::slot_snapshot`]) on every call, so an unknown or missing slot is a
//! clean tool error and a slot added at runtime works with no restart. There is
//! no auth — the `127.0.0.1` bind is the whole trust boundary.
//!
//! [`AppState::slot_snapshot`]: crate::telegram::bot::AppState::slot_snapshot
//!
//! # The two tools
//!
//! - **`send_file_to_user`** routes an outbound file (base64 `content`) to the
//!   slot-owning Telegram user: resolve `X-Slot → chat_id`
//!   ([`Db::chat_for_slot`](crate::persistence::Db::chat_for_slot)), then send the
//!   bytes through [`files::send_outbound_bytes`], wrapped in the `#25`
//!   flood-control/backoff retry ([`retry::with_retry`](crate::telegram::retry)).
//! - **`fetch_user_file`** is the inbound pull: the media path stores a
//!   downloaded file and announces its id to the model (#65 T6); the model calls
//!   this tool with that id, and the server [`take`](store::FileStore::take)s it
//!   from the store **scoped to the caller's slot** (cross-slot isolation lives in
//!   the store). Text comes back as a text block, an image as an image block (the
//!   vision path), and anything else as a short descriptive note.
//!
//! # Wiring
//!
//! [`build_router`] produces the single `/mcp` axum mount; `serve()` (#65 T7)
//! binds it and spawns the store's TTL sweep. This module does not touch
//! `serve()` itself.

pub mod store;

use std::sync::Arc;

use axum::http::request::Parts;
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use rmcp::{
    ErrorData, ServerHandler,
    handler::server::{router::tool::ToolRouter, tool::Extension, wrapper::Parameters},
    model::{CallToolResult, ContentBlock},
    tool, tool_handler, tool_router,
    transport::streamable_http_server::{
        StreamableHttpServerConfig, StreamableHttpService, session::local::LocalSessionManager,
    },
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use teloxide::types::ChatId;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::telegram::bot::AppState;
use crate::telegram::{files, retry};

/// Arguments for the `send_file_to_user` tool. The recipient is **not** here —
/// it is derived from the validated `X-Slot` header — so the model cannot address
/// another user's chat.
#[derive(Debug, Deserialize, Serialize, JsonSchema)]
struct SendFileArgs {
    /// The filename shown to the user. Its extension decides how the file
    /// arrives: an image extension is sent as a photo, everything else as a
    /// document.
    filename: String,
    /// The file's bytes, encoded as standard (RFC 4648) base64. To send text,
    /// base64-encode its UTF-8 bytes. An optional `data:<mime>;base64,` prefix is
    /// accepted and stripped.
    content: String,
    /// Optional caption sent alongside the file.
    caption: Option<String>,
}

/// Arguments for the `fetch_user_file` tool.
#[derive(Debug, Deserialize, Serialize, JsonSchema)]
struct FetchFileArgs {
    /// The file id from the announcement the proxy injected when the user sent a
    /// file. Each id is single-use and expires.
    id: String,
}

/// The shared, stateless MCP file-transfer server. One instance serves every
/// slot; the caller's slot is read per request from the `X-Slot` header (see the
/// module docs), never baked in and never a tool argument.
#[derive(Clone)]
pub struct FilesMcp {
    /// Shared dispatcher state — the slot registry (for `X-Slot` validation), the
    /// routing DB (`chat_for_slot`), the bot handle (outbound sends), and the file
    /// store (`fetch_user_file`).
    app: Arc<AppState>,
    /// The rmcp-generated tool dispatch table for this server.
    tool_router: ToolRouter<Self>,
}

#[tool_router]
impl FilesMcp {
    /// Build a server over the shared [`AppState`]. Called once per rmcp session
    /// by the [`StreamableHttpService`] factory in [`build_router`].
    pub fn new(app: Arc<AppState>) -> Self {
        Self {
            app,
            tool_router: Self::tool_router(),
        }
    }

    /// Read the caller's slot from the per-request `X-Slot` header and validate it
    /// against the **live** registry. A missing header and an unknown slot are
    /// both clean `invalid_params` tool errors; the request never proceeds on an
    /// unvalidated identity. Returns the validated slot name.
    fn slot_of(&self, parts: &Parts) -> Result<String, ErrorData> {
        let slot = parts
            .headers
            .get("x-slot")
            .and_then(|v| v.to_str().ok())
            .ok_or_else(|| ErrorData::invalid_params("missing X-Slot header", None))?;
        if self.app.slot_snapshot().iter().any(|s| s.name == slot) {
            Ok(slot.to_string())
        } else {
            Err(ErrorData::invalid_params(
                format!("unknown slot '{slot}'"),
                None,
            ))
        }
    }

    /// Send a file to the Telegram user you are working with.
    ///
    /// The recipient is whoever owns this workspace's slot — you do not choose it.
    /// Put the file's bytes in `content` as standard base64 (to send text,
    /// base64-encode its UTF-8 bytes); a `data:<mime>;base64,` prefix is accepted
    /// and stripped. `filename`'s extension decides whether it arrives as a photo
    /// (image extensions) or a document. `caption` is optional accompanying text.
    /// Returns "sent" once the file is delivered.
    #[tool(
        name = "send_file_to_user",
        description = "Send a file to the Telegram user you are working with. The recipient is fixed \
            (this workspace's slot) — you do not choose it. Set `content` to the file's bytes encoded \
            as standard base64; to send text, base64-encode its UTF-8 bytes. A `data:<mime>;base64,` \
            prefix is accepted and stripped. `filename`'s extension decides delivery: an image \
            extension is sent as a photo, otherwise as a document. `caption` is optional text shown \
            with the file. Returns \"sent\" on success."
    )]
    async fn send_file_to_user(
        &self,
        Extension(parts): Extension<Parts>,
        Parameters(args): Parameters<SendFileArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let slot = self.slot_of(&parts)?;

        let bytes = decode_content(&args.content).map_err(|err| {
            ErrorData::invalid_params(format!("`content` is not valid base64: {err}"), None)
        })?;

        let chat = match self.app.db.chat_for_slot(&slot) {
            Ok(Some(chat)) => chat,
            Ok(None) => {
                return Err(ErrorData::invalid_params(
                    "no Telegram user bound to this slot",
                    None,
                ));
            }
            Err(err) => {
                tracing::error!(slot = %slot, error = %err, "chat_for_slot lookup failed");
                return Err(ErrorData::internal_error(
                    "could not resolve recipient",
                    None,
                ));
            }
        };

        // Send through the #25 retry wrapper (429 flood-control + network backoff);
        // `send_outbound_bytes` returns `RequestError` by value so it can retry.
        // `bytes` is cloned per attempt (a fresh request each try).
        let caption = args.caption.as_deref();
        retry::with_retry("mcp_send_file", || {
            files::send_outbound_bytes(
                &self.app.bot,
                ChatId(chat),
                &args.filename,
                bytes.clone(),
                caption,
            )
        })
        .await
        .map_err(|err| {
            tracing::error!(slot = %slot, error = %err, "mcp send_file_to_user failed");
            ErrorData::internal_error(format!("failed to send file: {err}"), None)
        })?;

        Ok(CallToolResult::success(vec![ContentBlock::text("sent")]))
    }

    /// Fetch a file the user sent you, by the id from the proxy's announcement.
    ///
    /// When the user sends a file, the proxy adds a note to your prompt with the
    /// file's id; call this tool with that id to read the file **now**. Text files
    /// come back as text, images come back as the image itself (so you can see
    /// it), and other binary types come back as a short description. Each id is
    /// single-use and expires, so fetch it promptly.
    #[tool(
        name = "fetch_user_file",
        description = "Fetch a file the user sent you. When the user sends a file, the proxy adds a note \
            to your prompt containing the file's `id`; call this tool with that id to read the file's \
            contents NOW. A text file is returned as text, an image is returned as the image itself so \
            you can see it, and any other binary type is returned as a short description. Each id is \
            single-use and expires — fetch it promptly and only once."
    )]
    async fn fetch_user_file(
        &self,
        Extension(parts): Extension<Parts>,
        Parameters(args): Parameters<FetchFileArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let slot = self.slot_of(&parts)?;

        let id = Uuid::parse_str(args.id.trim())
            .map_err(|_| ErrorData::invalid_params("`id` is not a valid file id", None))?;

        let taken = match self.app.file_store.take(&slot, id).await {
            Ok(taken) => taken,
            Err(store::TakeError::NotFound) => {
                // Opaque on purpose — never reveal whether the id exists under
                // another slot (cross-slot isolation is enforced in the store).
                return Err(ErrorData::invalid_params(
                    "no such file, or it was already fetched or expired",
                    None,
                ));
            }
            Err(store::TakeError::Io(err)) => {
                tracing::error!(slot = %slot, error = %err, "reading fetched file failed");
                return Err(ErrorData::internal_error("could not read the file", None));
            }
        };

        let block = if taken.mime.starts_with("text/") {
            ContentBlock::text(String::from_utf8_lossy(&taken.bytes).into_owned())
        } else if taken.mime.starts_with("image/") {
            // The vision path: hand the model the image bytes as base64 + mime.
            ContentBlock::image(STANDARD.encode(&taken.bytes), taken.mime.clone())
        } else {
            ContentBlock::text(format!(
                "binary file {} ({}, {} bytes)",
                taken.filename,
                taken.mime,
                taken.bytes.len()
            ))
        };

        Ok(CallToolResult::success(vec![block]))
    }
}

// `router = self.tool_router` points the generated `call_tool`/`list_tools` at
// the cached [`ToolRouter`] field (built once in [`FilesMcp::new`]) rather than
// the default `Self::tool_router()`, which would rebuild the router on every
// call and leave the stored field unread (a dead-code warning under
// `clippy -D warnings`).
#[tool_handler(router = self.tool_router)]
impl ServerHandler for FilesMcp {}

/// Decode a `send_file_to_user` `content` argument into raw bytes.
///
/// The contract is standard (RFC 4648) base64. As a convenience an optional
/// `data:<mime>;base64,` URI prefix is stripped first — the base64 alphabet never
/// contains `;base64,`, so this can only ever match a genuine data-URI prefix and
/// never a chunk of real payload.
fn decode_content(content: &str) -> Result<Vec<u8>, base64::DecodeError> {
    let b64 = match content.split_once(";base64,") {
        Some((prefix, rest)) if prefix.starts_with("data:") => rest,
        _ => content,
    };
    STANDARD.decode(b64.trim())
}

/// Build the axum router hosting the single stateless MCP server.
///
/// One [`FilesMcp`] service is mounted at `/mcp` — **no** per-slot nesting; the
/// slot travels in the `X-Slot` header. The service runs stateless
/// (`with_stateful_mode(false)`, so there is no `MCP-Session-Id` bookkeeping) and
/// answers with JSON (`with_json_response(true)`). `ct` is the caller's
/// cancellation token, wired for graceful shutdown by `serve()` (#65 T7).
pub fn build_router(app: Arc<AppState>, ct: CancellationToken) -> axum::Router {
    let svc = StreamableHttpService::new(
        move || Ok(FilesMcp::new(app.clone())),
        Arc::new(LocalSessionManager::default()),
        StreamableHttpServerConfig::default()
            .with_stateful_mode(false)
            .with_json_response(true)
            .with_cancellation_token(ct),
    );
    axum::Router::new().nest_service("/mcp", svc)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_content_plain_base64() {
        // "hi" → "aGk=" in standard base64.
        assert_eq!(decode_content("aGk=").unwrap(), b"hi");
    }

    #[test]
    fn decode_content_strips_data_uri_prefix() {
        assert_eq!(
            decode_content("data:text/plain;base64,aGk=").unwrap(),
            b"hi"
        );
        assert_eq!(
            decode_content("data:image/png;base64,AAECAw==").unwrap(),
            &[0u8, 1, 2, 3]
        );
    }

    #[test]
    fn decode_content_tolerates_surrounding_whitespace() {
        assert_eq!(decode_content("  aGk=\n").unwrap(), b"hi");
    }

    #[test]
    fn decode_content_rejects_non_base64() {
        assert!(decode_content("not valid base64 !!!").is_err());
    }
}
