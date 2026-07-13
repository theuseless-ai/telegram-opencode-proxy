//! opencode wire types, derived from `fixtures/opencode/doc.json` (v1.17.18).
//!
//! We target the **V1 root-path** endpoints (`/session`, `/session/:id/message`,
//! `/permission/:id/reply`, …). V2 (`/api/*`) is also exposed by the server but
//! is not used here; the client keeps a thin V1/V2 path seam (see `client.rs`).
//!
//! Only the shapes the client needs right now are modelled. Unknown fields are
//! ignored on deserialize, so responses stay forward-compatible. See
//! `docs/design/architecture.md` §10. Issue #5.

// Forward-declared wire surface: parts of this API are consumed by the turn
// loop (#6), event relay (#7) and permission responder (#13). Exercised by the
// unit tests below; the `dead_code` allow keeps the not-yet-wired items green.
#![allow(dead_code)]

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::config::Model;

// ---------------------------------------------------------------------------
// Model selector — TWO wire shapes (validated in A0, §10)
// ---------------------------------------------------------------------------

/// Model object for `POST /session` — `{ id, providerID }`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateModel {
    /// The model id (opencode calls this `id` on the create endpoint).
    pub id: String,
    #[serde(rename = "providerID")]
    pub provider_id: String,
}

/// Model object for `POST /session/:id/message` — `{ providerID, modelID }`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PromptModel {
    #[serde(rename = "providerID")]
    pub provider_id: String,
    #[serde(rename = "modelID")]
    pub model_id: String,
}

impl From<&Model> for CreateModel {
    fn from(m: &Model) -> Self {
        Self {
            id: m.model_id.clone(),
            provider_id: m.provider_id.clone(),
        }
    }
}

impl From<&Model> for PromptModel {
    fn from(m: &Model) -> Self {
        Self {
            provider_id: m.provider_id.clone(),
            model_id: m.model_id.clone(),
        }
    }
}

// ---------------------------------------------------------------------------
// Requests
// ---------------------------------------------------------------------------

/// Body for `POST /session`. Both fields are optional per `doc.json`.
#[derive(Debug, Clone, Serialize)]
pub struct CreateSessionRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<CreateModel>,
}

/// Body for `POST /session/:id/message` (the blocking prompt).
#[derive(Debug, Clone, Serialize)]
pub struct PromptRequest {
    pub model: PromptModel,
    pub parts: Vec<PartInput>,
}

/// Input parts for a prompt: visible text and inbound files (#11). A file's
/// content rides in `url` as a base64 **data URI** (`data:<mime>;base64,…`),
/// which is what `FilePartInput` accepts (`{type:"file", mime, url, filename?}`).
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum PartInput {
    Text {
        text: String,
    },
    File {
        mime: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        filename: Option<String>,
        /// Data URI carrying the file bytes: `data:<mime>;base64,<base64>`.
        url: String,
    },
}

/// Body for `PATCH /session/:id` — only the permission ruleset is used here.
#[derive(Debug, Clone, Serialize)]
pub struct PatchSessionRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub permission: Option<Vec<PermissionRule>>,
}

/// Body for `POST /permission/:id/reply`.
#[derive(Debug, Clone, Serialize)]
pub struct PermissionReplyRequest {
    pub reply: PermissionReply,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

// ---------------------------------------------------------------------------
// Permission ruleset (shared by create/patch bodies and responses)
// ---------------------------------------------------------------------------

/// One entry of a `PermissionRuleset`: `{ permission, pattern, action }`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PermissionRule {
    /// The tool the rule applies to (e.g. `"bash"`).
    pub permission: String,
    /// Glob matched against the tool invocation (e.g. `"git commit*"`).
    pub pattern: String,
    pub action: PermissionAction,
}

/// `PermissionAction` enum from `doc.json`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PermissionAction {
    Allow,
    Deny,
    Ask,
}

/// `reply` values for `POST /permission/:id/reply`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PermissionReply {
    Once,
    Always,
    Reject,
}

// ---------------------------------------------------------------------------
// Provider catalogue (`GET /config/providers`) — startup model validation (#6)
// ---------------------------------------------------------------------------

/// Response from `GET /config/providers`. Shape (from `doc.json`):
/// `{ providers: [{ id, name, models: { <modelID>: {…} } }], default: {…} }`.
#[derive(Debug, Clone, Deserialize)]
pub struct ProvidersResponse {
    #[serde(default)]
    pub providers: Vec<ProviderInfo>,
    /// providerID → default modelID. Present in the wire but unused here.
    #[serde(default)]
    pub default: HashMap<String, String>,
}

/// One configured provider. We only need `id` + the `models` key set to
/// validate the proxy's `{provider_id, model_id}` selector; other fields
/// (`name`, `source`, `env`, `options`, …) are ignored.
#[derive(Debug, Clone, Deserialize)]
pub struct ProviderInfo {
    pub id: String,
    #[serde(default)]
    pub name: Option<String>,
    /// modelID → model metadata. Kept as an opaque value for forward-compat; the
    /// only field we reach into is `limit.context` (#72, see
    /// [`ProvidersResponse::context_limit`]).
    #[serde(default)]
    pub models: HashMap<String, serde_json::Value>,
}

impl ProvidersResponse {
    /// The context-window size (`models[model].limit.context`) opencode reports
    /// for `{provider_id, model_id}`, or `None` when the provider/model is absent
    /// or carries no positive limit. This is how a limit set in **opencode.json**
    /// reaches the context-usage footer (#72) without a proxy-side duplicate.
    pub fn context_limit(&self, provider_id: &str, model_id: &str) -> Option<u64> {
        self.providers
            .iter()
            .find(|p| p.id == provider_id)?
            .models
            .get(model_id)?
            .get("limit")?
            .get("context")?
            .as_u64()
            .filter(|&n| n > 0)
    }
}

// ---------------------------------------------------------------------------
// Responses
// ---------------------------------------------------------------------------

/// Response from `POST /session` (subset of the `Session` schema we care about).
#[derive(Debug, Clone, Deserialize)]
pub struct SessionResponse {
    pub id: String,
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub version: Option<String>,
}

/// One `{ info, parts }` message envelope, as returned by
/// `POST /session/:id/message` and each element of `GET /session/:id/message`.
#[derive(Debug, Clone, Deserialize)]
pub struct MessageEnvelope {
    pub info: MessageInfo,
    #[serde(default)]
    pub parts: Vec<Part>,
}

impl MessageEnvelope {
    /// Concatenated visible assistant text (skips reasoning/step/tool parts).
    pub fn text(&self) -> String {
        let mut out = String::new();
        for part in &self.parts {
            if let Part::Text { text, .. } = part {
                out.push_str(text);
            }
        }
        out
    }

    /// The visible text parts as `(part_id, text)`, in order. Used by the SSE
    /// reconnect backfill (`events::backfill`, #7) to reconcile missed deltas
    /// against a [`SeenParts`] dedup set keyed on the part id.
    ///
    /// [`SeenParts`]: crate::opencode::events::SeenParts
    pub fn text_parts(&self) -> impl Iterator<Item = (&str, &str)> {
        self.parts.iter().filter_map(|part| match part {
            Part::Text { id, text } => Some((id.as_str(), text.as_str())),
            _ => None,
        })
    }
}

/// Subset of a message's `info` block. Covers both user and assistant messages;
/// assistant-only fields are optional so either variant deserializes.
#[derive(Debug, Clone, Deserialize)]
pub struct MessageInfo {
    pub id: String,
    #[serde(rename = "sessionID")]
    pub session_id: String,
    #[serde(default)]
    pub role: Option<String>,
    #[serde(rename = "providerID", default)]
    pub provider_id: Option<String>,
    #[serde(rename = "modelID", default)]
    pub model_id: Option<String>,
    #[serde(default)]
    pub cost: Option<f64>,
    /// Token usage for an assistant message (absent on user messages). Drives the
    /// context-usage footer (#72).
    #[serde(default)]
    pub tokens: Option<Tokens>,
    #[serde(default)]
    pub finish: Option<String>,
}

/// Assistant token usage (opencode `Assistant.tokens`, #72). All counts default
/// to `0` so a provider that omits a field still deserializes.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
pub struct Tokens {
    #[serde(default)]
    pub input: u64,
    #[serde(default)]
    pub output: u64,
    #[serde(default)]
    pub reasoning: u64,
    #[serde(default)]
    pub cache: TokenCache,
}

/// The `cache` sub-object of [`Tokens`] — prompt-cache read/write counts.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
pub struct TokenCache {
    #[serde(default)]
    pub read: u64,
    #[serde(default)]
    pub write: u64,
}

impl Tokens {
    /// Context tokens currently consumed, mirroring opencode's own UI figure:
    /// `input + output + reasoning + cache.read + cache.write`. This is the
    /// numerator for the context-usage percentage (#72).
    pub fn context_used(&self) -> u64 {
        self.input + self.output + self.reasoning + self.cache.read + self.cache.write
    }
}

/// A message part. Only text/reasoning carry data we read; everything else
/// (`step-start`, `step-finish`, `tool`, …) collapses to `Other`. The part `id`
/// (present on both the message list and the `/global/event` stream) is kept on
/// the data-bearing variants so the SSE reconnect backfill can dedup by it (#7);
/// it defaults so a part that omits it still deserializes (forward-compatible).
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum Part {
    Text {
        #[serde(default)]
        id: String,
        text: String,
    },
    Reasoning {
        #[serde(default)]
        id: String,
        text: String,
    },
    #[serde(other)]
    Other,
}

#[cfg(test)]
mod tests {
    use super::*;

    const SESSION_CREATE: &str = include_str!("../../fixtures/opencode/session-create.json");
    const MESSAGE_RESPONSE: &str = include_str!("../../fixtures/opencode/message-response.json");
    const PATCH_PERMISSION: &str = include_str!("../../fixtures/opencode/patch-permission.json");

    #[test]
    fn create_model_serializes_id_and_provider_id() {
        let m = CreateModel {
            id: "Qwen3.6-35B-A3B-bf16".into(),
            provider_id: "llm-lan".into(),
        };
        let v: serde_json::Value = serde_json::to_value(&m).unwrap();
        assert_eq!(
            v,
            serde_json::json!({"id":"Qwen3.6-35B-A3B-bf16","providerID":"llm-lan"})
        );
    }

    #[test]
    fn prompt_model_serializes_provider_id_and_model_id() {
        let m = PromptModel {
            provider_id: "llm-lan".into(),
            model_id: "Qwen3.6-35B-A3B-bf16".into(),
        };
        let v: serde_json::Value = serde_json::to_value(&m).unwrap();
        assert_eq!(
            v,
            serde_json::json!({"providerID":"llm-lan","modelID":"Qwen3.6-35B-A3B-bf16"})
        );
    }

    #[test]
    fn model_selector_maps_from_config() {
        let cfg = Model {
            provider_id: "llm-lan".into(),
            model_id: "Qwen3.6-35B-A3B-bf16".into(),
            context_window: None,
        };
        assert_eq!(CreateModel::from(&cfg).id, "Qwen3.6-35B-A3B-bf16");
        assert_eq!(CreateModel::from(&cfg).provider_id, "llm-lan");
        assert_eq!(PromptModel::from(&cfg).model_id, "Qwen3.6-35B-A3B-bf16");
        assert_eq!(PromptModel::from(&cfg).provider_id, "llm-lan");
    }

    #[test]
    fn prompt_request_wire_shape() {
        let req = PromptRequest {
            model: PromptModel {
                provider_id: "llm-lan".into(),
                model_id: "m".into(),
            },
            parts: vec![PartInput::Text {
                text: "ping".into(),
            }],
        };
        let v: serde_json::Value = serde_json::to_value(&req).unwrap();
        assert_eq!(
            v,
            serde_json::json!({
                "model": {"providerID":"llm-lan","modelID":"m"},
                "parts": [{"type":"text","text":"ping"}]
            })
        );
    }

    #[test]
    fn file_part_input_wire_shape() {
        let with_name = PartInput::File {
            mime: "image/png".into(),
            filename: Some("shot.png".into()),
            url: "data:image/png;base64,AAAA".into(),
        };
        assert_eq!(
            serde_json::to_value(&with_name).unwrap(),
            serde_json::json!({
                "type": "file",
                "mime": "image/png",
                "filename": "shot.png",
                "url": "data:image/png;base64,AAAA"
            })
        );
        // `filename` is omitted when absent.
        let no_name = PartInput::File {
            mime: "text/plain".into(),
            filename: None,
            url: "data:,x".into(),
        };
        assert_eq!(
            serde_json::to_value(&no_name).unwrap(),
            serde_json::json!({ "type": "file", "mime": "text/plain", "url": "data:,x" })
        );
    }

    #[test]
    fn create_request_omits_none_fields() {
        let req = CreateSessionRequest {
            title: None,
            model: None,
        };
        let v: serde_json::Value = serde_json::to_value(&req).unwrap();
        assert_eq!(v, serde_json::json!({}));
    }

    #[test]
    fn permission_rule_serializes_deny() {
        let rule = PermissionRule {
            permission: "bash".into(),
            pattern: "git commit*".into(),
            action: PermissionAction::Deny,
        };
        let v: serde_json::Value = serde_json::to_value(&rule).unwrap();
        assert_eq!(
            v,
            serde_json::json!({"permission":"bash","pattern":"git commit*","action":"deny"})
        );
    }

    #[test]
    fn permission_reply_reject_serializes() {
        let req = PermissionReplyRequest {
            reply: PermissionReply::Reject,
            message: None,
        };
        let v: serde_json::Value = serde_json::to_value(&req).unwrap();
        assert_eq!(v, serde_json::json!({"reply":"reject"}));
    }

    #[test]
    fn deserializes_session_create_fixture() {
        let s: SessionResponse = serde_json::from_str(SESSION_CREATE).unwrap();
        assert_eq!(s.id, "ses_0b08a450affewL7B8cwPD7l3y6");
        assert_eq!(s.title.as_deref(), Some("a0-spike-plain"));
        assert_eq!(s.version.as_deref(), Some("1.17.18"));
    }

    #[test]
    fn deserializes_message_response_fixture_and_extracts_text() {
        let m: MessageEnvelope = serde_json::from_str(MESSAGE_RESPONSE).unwrap();
        assert_eq!(m.info.id, "msg_f4f75bb55001ZAKZrMNmabgGWd");
        assert_eq!(m.info.session_id, "ses_0b08a450affewL7B8cwPD7l3y6");
        assert_eq!(m.info.provider_id.as_deref(), Some("deepseek"));
        assert_eq!(m.info.model_id.as_deref(), Some("deepseek-v4-flash"));
        assert_eq!(m.info.finish.as_deref(), Some("stop"));
        // `text()` skips the step-start / reasoning / step-finish parts.
        assert_eq!(m.text(), "PONG");
        // Assistant token usage rides on `info.tokens` (#72): the fixture reports
        // input 10142 + output 3 + reasoning 15 + cache 0 = 10160 context tokens.
        let tokens = m.info.tokens.expect("assistant message carries tokens");
        assert_eq!(tokens.input, 10142);
        assert_eq!(tokens.context_used(), 10_160);
    }

    #[test]
    fn tokens_context_used_sums_all_components() {
        let t = Tokens {
            input: 100,
            output: 20,
            reasoning: 5,
            cache: TokenCache {
                read: 30,
                write: 10,
            },
        };
        assert_eq!(t.context_used(), 165);
    }

    #[test]
    fn message_without_tokens_deserializes_with_none() {
        // A user message (no `tokens` key) must still parse.
        let info: MessageInfo = serde_json::from_value(serde_json::json!({
            "id": "msg_x",
            "sessionID": "ses_x",
            "role": "user"
        }))
        .expect("user message info parses");
        assert!(info.tokens.is_none());
    }

    #[test]
    fn deserializes_patch_permission_fixture_ruleset() {
        // The gated fixture PATCHes `bash = ask`; verify the ruleset round-trips.
        #[derive(Deserialize)]
        struct WithPermission {
            permission: Vec<PermissionRule>,
        }
        let p: WithPermission = serde_json::from_str(PATCH_PERMISSION).unwrap();
        assert_eq!(p.permission.len(), 1);
        assert_eq!(p.permission[0].permission, "bash");
        assert_eq!(p.permission[0].pattern, "*");
        assert_eq!(p.permission[0].action, PermissionAction::Ask);
    }
}
