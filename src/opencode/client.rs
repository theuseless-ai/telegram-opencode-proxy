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

use anyhow::{Context, Result, bail};
use reqwest::StatusCode;

use crate::config::Slot;

use super::types::{
    CreateModel, CreateSessionRequest, MessageEnvelope, PartInput, PatchSessionRequest,
    PermissionReplyRequest, PermissionRule, PromptModel, PromptRequest, ProvidersResponse,
    SessionResponse,
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

    /// The instance base URL (trailing slash stripped). Test/diagnostic helper.
    #[allow(dead_code)] // exercised by unit tests; not needed on the live path.
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// V1 path seam — join the base URL with a root-relative endpoint `path`.
    fn url(&self, path: &str) -> String {
        format!("{}{}", self.base_url, path)
    }

    /// `GET /config/providers` — the instance's configured provider catalogue.
    /// Used at startup to fail fast if the proxy's `{provider_id, model_id}`
    /// selector does not resolve on this instance (see [`validate_model`]).
    pub async fn config_providers(&self) -> Result<ProvidersResponse> {
        let resp = self
            .http
            .get(self.url("/config/providers"))
            .send()
            .await
            .context("GET /config/providers")?
            .error_for_status()
            .context("GET /config/providers returned an error status")?;
        resp.json::<ProvidersResponse>()
            .await
            .context("decoding /config/providers response")
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
    /// assistant message once opencode finishes the turn. `parts` is the prompt
    /// body: text and/or inbound files (#11).
    pub async fn prompt(
        &self,
        session_id: &str,
        model: PromptModel,
        parts: Vec<PartInput>,
    ) -> Result<MessageEnvelope> {
        let body = PromptRequest { model, parts };
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

    /// `GET /session/:id/message` — list the session's messages. Not needed by
    /// the blocking turn loop; used by the SSE reconnect/backfill path
    /// ([`events::backfill`](crate::opencode::events::backfill), #7).
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

    /// `POST /session/:id/abort` — interrupt the session's in-flight turn (the
    /// `/stop` command, #9). Returns opencode's boolean result (`true` = aborted).
    /// No request body; the blocking `prompt` for that session then unblocks.
    pub async fn abort_session(&self, session_id: &str) -> Result<bool> {
        let resp = self
            .http
            .post(self.url(&format!("/session/{session_id}/abort")))
            .send()
            .await
            .context("POST /session/:id/abort")?
            .error_for_status()
            .context("POST /session/:id/abort returned an error status")?;
        resp.json::<bool>().await.context("decoding abort response")
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
    #[allow(dead_code)]
    pub async fn reply_permission(
        &self,
        _request_id: &str,
        _reply: PermissionReplyRequest,
    ) -> Result<bool> {
        bail!("not implemented (#13)")
    }

    /// `GET /file/content` — read a file from the instance workdir. Stubbed
    /// until outbound file support (#13).
    #[allow(dead_code)]
    pub async fn read_file(&self, _path: &str) -> Result<String> {
        bail!("not implemented (#13)")
    }
}

/// Verify the configured `{provider_id, model_id}` exists in a
/// `/config/providers` response. On mismatch, returns an error that lists what
/// IS available so the operator can fix `config.toml` / `opencode.json`.
pub fn validate_model(
    providers: &ProvidersResponse,
    provider_id: &str,
    model_id: &str,
) -> Result<()> {
    let Some(provider) = providers.providers.iter().find(|p| p.id == provider_id) else {
        let available: Vec<&str> = providers.providers.iter().map(|p| p.id.as_str()).collect();
        bail!(
            "provider '{provider_id}' is not configured in opencode — \
             available providers: [{}]. Fix `[model].provider_id` in config.toml \
             or register the provider in opencode.json (architecture.md §12).",
            available.join(", ")
        );
    };
    if !provider.models.contains_key(model_id) {
        let mut models: Vec<&str> = provider.models.keys().map(String::as_str).collect();
        models.sort_unstable();
        bail!(
            "model '{model_id}' is not configured under provider '{provider_id}' — \
             available models: [{}]. Fix `[model].model_id` in config.toml or add \
             the model under that provider in opencode.json (architecture.md §12).",
            models.join(", ")
        );
    }
    Ok(())
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

    const PROVIDERS: &str = include_str!("../../fixtures/opencode/config-providers.json");

    fn providers() -> ProvidersResponse {
        serde_json::from_str(PROVIDERS).expect("providers fixture parses")
    }

    #[test]
    fn deserializes_providers_fixture() {
        let p = providers();
        assert_eq!(p.providers.len(), 1);
        assert_eq!(p.providers[0].id, "llm-lan");
        assert!(p.providers[0].models.contains_key("Qwen3.6-35B-A3B-bf16"));
        assert_eq!(
            p.default.get("llm-lan").map(String::as_str),
            Some("Qwen3.6-35B-A3B-bf16")
        );
    }

    #[test]
    fn validate_model_accepts_configured_selector() {
        assert!(validate_model(&providers(), "llm-lan", "Qwen3.6-35B-A3B-bf16").is_ok());
    }

    #[test]
    fn validate_model_rejects_unknown_provider() {
        let err = validate_model(&providers(), "nope", "Qwen3.6-35B-A3B-bf16")
            .unwrap_err()
            .to_string();
        assert!(err.contains("provider 'nope'"), "{err}");
        // Error lists what IS available.
        assert!(err.contains("llm-lan"), "{err}");
    }

    #[test]
    fn validate_model_rejects_unknown_model() {
        let err = validate_model(&providers(), "llm-lan", "ghost-model")
            .unwrap_err()
            .to_string();
        assert!(err.contains("model 'ghost-model'"), "{err}");
        assert!(err.contains("Qwen3.6-35B-A3B-bf16"), "{err}");
    }

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
