//! reqwest client bound to one opencode instance base URL.
//!
//! Implemented now (issue #5): `create_session`, `prompt` (blocking
//! `POST /session/:id/message`), `get_messages`, `session_exists`, and
//! `patch_permission` (used by `session.rs` to set the deny posture on create).
//! `reply_permission` and `read_file` are deliberately stubbed until #13.
//!
//! The `model` object differs by endpoint — `{id, providerID}` on create vs
//! `{providerID, modelID}` on message — handled by the two types in `types.rs`.
//!
//! The base URL is injectable (`new`) so the client is unit-testable without a
//! live server. V1/V2 seam: all paths are built through `url()`; a V2 adapter
//! would prefix `/api` there. We target V1. See `architecture.md` §10.

// Forward-declared client surface: `prompt`/`get_messages` are wired by the
// turn loop (#6) and the stubs by #13. Keeps the not-yet-called methods green.
#![allow(dead_code)]

use anyhow::{Context, Result, bail};
use reqwest::StatusCode;

use crate::config::Slot;

use super::types::{
    CreateModel, CreateSessionRequest, MessageEnvelope, PartInput, PatchSessionRequest,
    PermissionReplyRequest, PermissionRule, PromptModel, PromptRequest, SessionResponse,
};

/// An async client for a single opencode instance.
#[derive(Debug, Clone)]
pub struct OpencodeClient {
    /// Base URL with any trailing slash stripped (e.g. `http://127.0.0.1:4096`).
    base_url: String,
    http: reqwest::Client,
}

impl OpencodeClient {
    /// Build a client for `base_url`. The URL is injectable so tests can point
    /// the client at a mock server.
    pub fn new(base_url: impl Into<String>) -> Result<Self> {
        let http = reqwest::Client::builder()
            .build()
            .context("building reqwest client")?;
        Ok(Self {
            base_url: normalize_base(base_url.into()),
            http,
        })
    }

    /// Build a client for a configured slot.
    pub fn for_slot(slot: &Slot) -> Result<Self> {
        Self::new(slot.opencode_url.clone())
    }

    /// The instance base URL (trailing slash stripped).
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// V1 path seam — join the base URL with a root-relative endpoint `path`.
    fn url(&self, path: &str) -> String {
        format!("{}{}", self.base_url, path)
    }

    /// `POST /session` — create a session, optionally with a title and model.
    pub async fn create_session(
        &self,
        title: Option<String>,
        model: Option<CreateModel>,
    ) -> Result<SessionResponse> {
        let body = CreateSessionRequest { title, model };
        let resp = self
            .http
            .post(self.url("/session"))
            .json(&body)
            .send()
            .await
            .context("POST /session")?
            .error_for_status()
            .context("POST /session returned an error status")?;
        resp.json::<SessionResponse>()
            .await
            .context("decoding session-create response")
    }

    /// `POST /session/:id/message` — **blocking**; returns the completed
    /// assistant message once opencode finishes the turn.
    pub async fn prompt(
        &self,
        session_id: &str,
        model: PromptModel,
        text: impl Into<String>,
    ) -> Result<MessageEnvelope> {
        let body = PromptRequest {
            model,
            parts: vec![PartInput::Text { text: text.into() }],
        };
        let resp = self
            .http
            .post(self.url(&format!("/session/{session_id}/message")))
            .json(&body)
            .send()
            .await
            .context("POST /session/:id/message")?
            .error_for_status()
            .context("POST /session/:id/message returned an error status")?;
        resp.json::<MessageEnvelope>()
            .await
            .context("decoding prompt response")
    }

    /// `GET /session/:id/message` — list the session's messages.
    pub async fn get_messages(&self, session_id: &str) -> Result<Vec<MessageEnvelope>> {
        let resp = self
            .http
            .get(self.url(&format!("/session/{session_id}/message")))
            .send()
            .await
            .context("GET /session/:id/message")?
            .error_for_status()
            .context("GET /session/:id/message returned an error status")?;
        resp.json::<Vec<MessageEnvelope>>()
            .await
            .context("decoding message list")
    }

    /// `GET /session/:id` — probe whether opencode still knows this session.
    /// A `404` (e.g. a wiped opencode DB) maps to `Ok(false)` so callers can
    /// transparently recreate; other non-success statuses are errors.
    pub async fn session_exists(&self, session_id: &str) -> Result<bool> {
        let resp = self
            .http
            .get(self.url(&format!("/session/{session_id}")))
            .send()
            .await
            .context("GET /session/:id")?;
        match resp.status() {
            StatusCode::NOT_FOUND => Ok(false),
            s if s.is_success() => Ok(true),
            s => bail!("GET /session/{session_id} returned {s}"),
        }
    }

    /// `PATCH /session/:id` with a permission ruleset. Used by `session.rs` to
    /// install the deliberate `deny` posture on create (§2.6). #13 will reuse
    /// this to flip rules to `ask` once an interactive responder exists.
    pub async fn patch_permission(
        &self,
        session_id: &str,
        rules: Vec<PermissionRule>,
    ) -> Result<()> {
        let body = PatchSessionRequest {
            permission: Some(rules),
        };
        self.http
            .patch(self.url(&format!("/session/{session_id}")))
            .json(&body)
            .send()
            .await
            .context("PATCH /session/:id")?
            .error_for_status()
            .context("PATCH /session/:id returned an error status")?;
        Ok(())
    }

    /// `POST /permission/:id/reply` — stubbed until the permission responder
    /// lands in #13.
    pub async fn reply_permission(
        &self,
        _request_id: &str,
        _reply: PermissionReplyRequest,
    ) -> Result<bool> {
        bail!("not implemented (#13)")
    }

    /// `GET /file/content` — read a file from the instance workdir. Stubbed
    /// until outbound file support (#13).
    pub async fn read_file(&self, _path: &str) -> Result<String> {
        bail!("not implemented (#13)")
    }
}

/// Strip trailing slashes from a base URL so `url()` never doubles them.
fn normalize_base(mut s: String) -> String {
    while s.ends_with('/') {
        s.pop();
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_trailing_slashes() {
        assert_eq!(normalize_base("http://x:4096/".into()), "http://x:4096");
        assert_eq!(normalize_base("http://x:4096///".into()), "http://x:4096");
        assert_eq!(normalize_base("http://x:4096".into()), "http://x:4096");
    }

    #[test]
    fn builds_v1_paths_off_base_url() {
        let c = OpencodeClient::new("http://127.0.0.1:4096/").unwrap();
        assert_eq!(c.base_url(), "http://127.0.0.1:4096");
        assert_eq!(c.url("/session"), "http://127.0.0.1:4096/session");
        assert_eq!(
            c.url("/session/ses_abc/message"),
            "http://127.0.0.1:4096/session/ses_abc/message"
        );
    }

    #[tokio::test]
    async fn stubs_bail_with_issue_reference() {
        let c = OpencodeClient::new("http://127.0.0.1:4096").unwrap();
        let e = c
            .reply_permission(
                "per_x",
                PermissionReplyRequest {
                    reply: super::super::types::PermissionReply::Reject,
                    message: None,
                },
            )
            .await
            .unwrap_err();
        assert!(e.to_string().contains("#13"));
        let e = c.read_file("/etc/hosts").await.unwrap_err();
        assert!(e.to_string().contains("#13"));
    }
}
