//! Inbound: Telegram photo/doc → base64 `FilePart` (data URI). Outbound:
//! `send_document`/`send_photo` by mime; `/get <path>` with within-workdir guard.
//! See `docs/design/architecture.md` §2.4/§2.5. Issues #11/#12.
//!
//! #11 implements the **inbound** half: [`inbound_parts`] turns a media message
//! into the prompt parts for a turn — the file as a base64 data-URI
//! [`PartInput::File`], plus any caption as text. Outbound (#12) is still ahead.

use anyhow::{Context, Result, bail};
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use futures_util::StreamExt;
use teloxide::net::Download;
use teloxide::prelude::*;
use teloxide::types::{FileId, Message};

use crate::opencode::types::PartInput;

/// Largest inbound file we accept. Telegram's own `getFile` caps downloads at
/// 20 MB, so this is really a guard for the pre-download size check.
const MAX_INBOUND_BYTES: u32 = 20 * 1024 * 1024;

/// The attachment picked from a message: which file to fetch and how to label it.
struct Attachment {
    file_id: FileId,
    mime: String,
    filename: Option<String>,
    size: u32,
}

/// Select the attachment to send opencode: the largest photo size, or a
/// document. `None` if the message carries neither.
fn pick_attachment(msg: &Message) -> Option<Attachment> {
    if let Some(sizes) = msg.photo() {
        // Photos arrive as ascending thumbnails; take the largest.
        let largest = sizes.iter().max_by_key(|p| p.file.size)?;
        return Some(Attachment {
            file_id: largest.file.id.clone(),
            mime: "image/jpeg".to_string(),
            filename: Some("photo.jpg".to_string()),
            size: largest.file.size,
        });
    }
    if let Some(doc) = msg.document() {
        let telegram_mime = doc.mime_type.as_ref().map(|m| m.to_string());
        return Some(Attachment {
            file_id: doc.file.id.clone(),
            mime: resolve_mime(telegram_mime.as_deref(), doc.file_name.as_deref()),
            filename: doc.file_name.clone(),
            size: doc.file.size,
        });
    }
    None
}

/// Choose the MIME to attach a document with. A **useful** MIME from Telegram
/// wins; otherwise (missing, or the useless `application/octet-stream` default)
/// we infer it from the file extension. This matters because opencode only
/// inlines a file's content when it has a real (esp. `text/*`) MIME — an
/// `application/octet-stream` part yields an **empty** model reply (#11 fix).
fn resolve_mime(telegram_mime: Option<&str>, filename: Option<&str>) -> String {
    if let Some(mime) = telegram_mime
        && !mime.is_empty()
        && mime != "application/octet-stream"
    {
        return mime.to_string();
    }
    mime_from_extension(filename).unwrap_or_else(|| "application/octet-stream".to_string())
}

/// Infer a MIME from a filename's extension, for the common text/code/doc types
/// (and images). `None` for unknown extensions.
fn mime_from_extension(filename: Option<&str>) -> Option<String> {
    let ext = filename?
        .rsplit_once('.')
        .map(|(_, ext)| ext.to_ascii_lowercase())?;
    let mime = match ext.as_str() {
        "txt" | "text" | "log" => "text/plain",
        "md" | "markdown" => "text/markdown",
        "html" | "htm" => "text/html",
        "css" => "text/css",
        "csv" => "text/csv",
        "json" => "application/json",
        "xml" => "application/xml",
        "yaml" | "yml" => "application/yaml",
        // Config + source files: opencode just needs a text/* MIME to inline them.
        "toml" | "ini" | "cfg" | "conf" | "env" => "text/plain",
        "js" | "mjs" | "cjs" | "ts" | "tsx" | "jsx" => "text/plain",
        "rs" | "go" | "py" | "rb" | "php" | "java" | "kt" | "c" | "h" | "cpp" | "hpp" | "cc"
        | "cs" | "swift" | "sh" | "bash" | "zsh" | "sql" | "lua" | "r" => "text/plain",
        "pdf" => "application/pdf",
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "gif" => "image/gif",
        "webp" => "image/webp",
        _ => return None,
    };
    Some(mime.to_string())
}

/// Build the prompt parts for an inbound media message (#11): download the
/// photo/document, base64-encode it as a data-URI [`PartInput::File`], and append
/// any caption as a text part. `Ok(None)` if the message carries no file.
pub async fn inbound_parts(bot: &Bot, msg: &Message) -> Result<Option<Vec<PartInput>>> {
    let Some(att) = pick_attachment(msg) else {
        return Ok(None);
    };
    if att.size > MAX_INBOUND_BYTES {
        bail!(
            "file is {} MB; the limit is {} MB",
            att.size / (1024 * 1024),
            MAX_INBOUND_BYTES / (1024 * 1024)
        );
    }

    // Resolve the download path, then stream the bytes into memory.
    let file = bot.get_file(att.file_id).await.context("getFile")?;
    let mut bytes = Vec::with_capacity(att.size as usize);
    let mut stream = bot.download_file_stream(&file.path);
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.context("downloading file chunk")?;
        bytes.extend_from_slice(&chunk);
    }

    let mut parts = vec![PartInput::File {
        mime: att.mime.clone(),
        filename: att.filename,
        url: data_uri(&att.mime, &bytes),
    }];
    // A caption becomes a text part alongside the file.
    if let Some(caption) = msg.caption()
        && !caption.trim().is_empty()
    {
        parts.push(PartInput::Text {
            text: caption.to_string(),
        });
    }
    Ok(Some(parts))
}

/// A `data:<mime>;base64,<…>` URI carrying `bytes`.
fn data_uri(mime: &str, bytes: &[u8]) -> String {
    format!("data:{mime};base64,{}", STANDARD.encode(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn data_uri_is_base64_data_uri() {
        // "hi" → "aGk=" in standard base64.
        assert_eq!(data_uri("text/plain", b"hi"), "data:text/plain;base64,aGk=");
        assert_eq!(
            data_uri("image/png", &[0, 1, 2, 3]),
            "data:image/png;base64,AAECAw=="
        );
    }

    /// A photo message deserialized from the Bot API wire shape; `pick_attachment`
    /// selects the largest size and labels it as a JPEG.
    #[test]
    fn pick_attachment_takes_the_largest_photo() {
        let msg: Message = serde_json::from_value(json!({
            "message_id": 1,
            "date": 0,
            "chat": { "id": 7, "type": "private", "first_name": "T" },
            "from": { "id": 7, "is_bot": false, "first_name": "T" },
            "photo": [
                { "file_id": "small", "file_unique_id": "us", "file_size": 100, "width": 90, "height": 90 },
                { "file_id": "big",   "file_unique_id": "ub", "file_size": 9000, "width": 1280, "height": 1280 }
            ],
            "caption": "look"
        }))
        .expect("photo message parses");

        let att = pick_attachment(&msg).expect("an attachment");
        assert_eq!(att.file_id.0, "big", "largest size chosen");
        assert_eq!(att.mime, "image/jpeg");
        assert_eq!(att.filename.as_deref(), Some("photo.jpg"));
    }

    #[test]
    fn resolve_mime_infers_from_extension_when_octet_stream() {
        // The useless octet-stream default is overridden by the extension.
        assert_eq!(
            resolve_mime(Some("application/octet-stream"), Some("index.html")),
            "text/html"
        );
        assert_eq!(resolve_mime(None, Some("notes.md")), "text/markdown");
        assert_eq!(resolve_mime(None, Some("data.json")), "application/json");
        assert_eq!(resolve_mime(None, Some("main.rs")), "text/plain");
        // A genuine MIME from Telegram is kept as-is.
        assert_eq!(
            resolve_mime(Some("application/pdf"), Some("x.bin")),
            "application/pdf"
        );
        // Truly unknown → octet-stream.
        assert_eq!(
            resolve_mime(None, Some("archive.xyz")),
            "application/octet-stream"
        );
        assert_eq!(resolve_mime(None, None), "application/octet-stream");
    }

    #[test]
    fn pick_attachment_is_none_for_plain_text() {
        let msg: Message = serde_json::from_value(json!({
            "message_id": 1,
            "date": 0,
            "chat": { "id": 7, "type": "private", "first_name": "T" },
            "from": { "id": 7, "is_bot": false, "first_name": "T" },
            "text": "hello"
        }))
        .expect("text message parses");
        assert!(pick_attachment(&msg).is_none());
    }
}
