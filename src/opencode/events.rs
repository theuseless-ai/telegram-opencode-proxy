//! opencode event relay: subscribe `/global/event` (SSE), parse the wrapped
//! frame `{directory, project, payload:{id, type, properties}}` into a typed
//! [`Event`], reconnect transparently, and reconcile missed deltas by part id.
//!
//! This is the **opencode side** of the return path (§7/§13, issue #7). It hands
//! typed [`Event`]s to the Telegram streaming renderer (#8) — it does no Telegram
//! I/O itself. The wire names are the A0-validated set (`architecture.md` §10):
//! `message.part.delta` (streaming text), `message.part.updated` (part lifecycle
//! incl. tool state), `session.status` (busy/idle → typing liveness),
//! `permission.asked` (the gate, #13), plus `server.connected`. The event-store
//! mirror frames — `payload.type == "sync"` and the `*.1` suffixed types — are
//! **dropped** so a part is never emitted twice.
//!
//! ## Reconnect & dedup
//!
//! [`Subscription`] wraps [`reqwest_eventsource::EventSource`] with a
//! [`Constant`](reqwest_eventsource::retry::Constant) retry policy: when the
//! stream ends it re-opens on the next poll, so [`Subscription::recv`] is an
//! endless typed-event source. Across a reconnect opencode re-sends the in-flight
//! message's parts; [`SeenParts`] (keyed on part id) lets the consumer skip parts
//! it has already rendered, and [`backfill`] fetches `GET /session/:id/message`
//! to recover deltas that arrived during the gap.

use std::collections::HashSet;
use std::time::Duration;

use anyhow::{Context, Result};
use futures_util::StreamExt;
use reqwest_eventsource::retry::Constant;
use reqwest_eventsource::{Event as EsEvent, EventSource};
use serde::Deserialize;

use super::client::OpencodeClient;

// ---------------------------------------------------------------------------
// Typed event model
// ---------------------------------------------------------------------------

/// A surfaced opencode event, decoded from one `/global/event` SSE frame.
///
/// Control/mirror frames (`sync`, `*.1`, `server.heartbeat`) never become an
/// `Event` — [`parse_frame`] drops them. Frames we recognise but don't act on
/// yet (`message.updated`, `session.diff`, …) collapse to [`Event::Other`] so a
/// caller can log the whole stream uniformly.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    /// `server.connected` — the (re)subscription is live. Emitted on every
    /// successful (re)connect, so seeing it twice means a reconnect happened.
    Connected,
    /// `message.part.delta` — a streaming text/reasoning chunk for one part.
    Delta(Delta),
    /// `message.part.updated` — a part's lifecycle changed (step boundary,
    /// reasoning start, or a tool moving pending→running→completed/error).
    PartUpdated(PartUpdate),
    /// `session.status` — the session went busy/idle (drives `typing`, §13).
    Status {
        session_id: String,
        status: SessionStatus,
    },
    /// `permission.asked` — the agent turn is blocked on a gate (#13).
    Permission(PermissionAsked),
    /// A recognised-but-unhandled frame; `kind` is the wire `payload.type`.
    Other { kind: String },
}

/// A `message.part.delta` — one streamed chunk appended to part `part_id`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct Delta {
    #[serde(rename = "sessionID")]
    pub session_id: String,
    #[serde(rename = "messageID")]
    pub message_id: String,
    #[serde(rename = "partID")]
    pub part_id: String,
    /// The part field the delta targets — `"text"` for both visible text and
    /// reasoning parts (the part's kind is known from its `PartUpdated`).
    pub field: String,
    pub delta: String,
}

/// A `message.part.updated` — a part's current state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartUpdate {
    pub session_id: String,
    pub message_id: String,
    pub part_id: String,
    pub kind: PartKind,
}

/// The kind of a part carried by a [`PartUpdate`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PartKind {
    StepStart,
    Reasoning,
    Text,
    /// A tool invocation and its lifecycle status.
    Tool {
        name: String,
        call_id: String,
        status: ToolStatus,
        /// `state.title` — opencode's own human-readable summary of the call
        /// (e.g. the command for `bash`, the sub-agent for `task`), when present.
        /// Drives the Verbose arg line and the sub-agent tag (#14); `None` on the
        /// pending state or when opencode omits it.
        title: Option<String>,
    },
    /// Any other part `type` we don't special-case; carries the raw wire value.
    Other(String),
}

/// A tool part's lifecycle status (`state.status` on the wire).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ToolStatus {
    Pending,
    Running,
    Completed,
    Error,
    Other(String),
}

impl ToolStatus {
    fn from_wire(s: &str) -> Self {
        match s {
            "pending" => Self::Pending,
            "running" => Self::Running,
            "completed" => Self::Completed,
            "error" => Self::Error,
            other => Self::Other(other.to_string()),
        }
    }
}

/// A session's busy/idle status from `session.status`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionStatus {
    Busy,
    Idle,
    Other(String),
}

/// A `permission.asked` gate. The agent turn stays blocked in opencode until
/// replied to (`POST /permission/:id/reply`, wired in #13).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PermissionAsked {
    /// The permission request id (`per_…`) — the reply target.
    pub id: String,
    pub session_id: String,
    /// The tool being gated (e.g. `"bash"`).
    pub permission: String,
    /// The invocation patterns matched (e.g. `["echo hi"]`).
    pub patterns: Vec<String>,
    /// `metadata.command` — the concrete command, when present.
    pub command: Option<String>,
    /// The originating message / tool-call ids, when present.
    pub message_id: Option<String>,
    pub call_id: Option<String>,
}

// ---------------------------------------------------------------------------
// Frame parsing
// ---------------------------------------------------------------------------

/// The event body — `{id, type, properties}`. On `/global/event` it is nested
/// under a `{directory, project, payload}` wrapper; the directory-scoped `/event`
/// stream emits it bare. [`parse_frame`] unwraps the former transparently.
#[derive(Deserialize)]
struct Payload {
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    properties: serde_json::Value,
}

/// Parse one SSE `data:` payload into a typed [`Event`].
///
/// - `Ok(Some(event))` — a surfaced event.
/// - `Ok(None)` — an intentionally-dropped frame (event-store `sync` mirror, a
///   `*.1` mirror type, or `server.heartbeat`).
/// - `Err(_)` — malformed JSON; the [`Subscription`] logs and skips these rather
///   than tearing down the stream.
pub fn parse_frame(data: &str) -> Result<Option<Event>, serde_json::Error> {
    let root: serde_json::Value = serde_json::from_str(data)?;
    // `/global/event` nests the event under `payload`; the directory-scoped
    // `/event` stream emits it bare. Accept both.
    let body = match root {
        serde_json::Value::Object(mut map) => map.remove("payload").unwrap_or(map.into()),
        other => other,
    };
    let payload: Payload = serde_json::from_value(body)?;
    let kind = payload.kind;
    let props = payload.properties;

    // Drop the event-store mirror: the `sync` wrapper and the `*.1` duplicate
    // types both re-carry an already-surfaced event.
    if kind == "sync" || kind.ends_with(".1") {
        return Ok(None);
    }

    let event = match kind.as_str() {
        "server.connected" => Event::Connected,
        "server.heartbeat" => return Ok(None),
        "message.part.delta" => Event::Delta(serde_json::from_value(props)?),
        "message.part.updated" => Event::PartUpdated(parse_part_update(props)?),
        "session.status" => parse_status(props)?,
        "permission.asked" => Event::Permission(parse_permission(props)?),
        _ => Event::Other { kind },
    };
    Ok(Some(event))
}

/// `message.part.updated.properties` → [`PartUpdate`].
fn parse_part_update(props: serde_json::Value) -> Result<PartUpdate, serde_json::Error> {
    #[derive(Deserialize)]
    struct Props {
        part: WirePart,
    }
    #[derive(Deserialize)]
    struct WirePart {
        id: String,
        #[serde(rename = "messageID")]
        message_id: String,
        #[serde(rename = "sessionID")]
        session_id: String,
        #[serde(rename = "type")]
        ptype: String,
        #[serde(default)]
        tool: Option<String>,
        #[serde(rename = "callID", default)]
        call_id: Option<String>,
        #[serde(default)]
        state: Option<ToolStateWire>,
    }
    #[derive(Deserialize)]
    struct ToolStateWire {
        #[serde(default)]
        status: Option<String>,
        #[serde(default)]
        title: Option<String>,
    }

    let p: Props = serde_json::from_value(props)?;
    let part = p.part;
    let kind = match part.ptype.as_str() {
        "step-start" => PartKind::StepStart,
        "reasoning" => PartKind::Reasoning,
        "text" => PartKind::Text,
        "tool" => {
            let (status, title) = match part.state {
                Some(s) => (
                    s.status
                        .map(|s| ToolStatus::from_wire(&s))
                        .unwrap_or(ToolStatus::Pending),
                    s.title,
                ),
                None => (ToolStatus::Pending, None),
            };
            PartKind::Tool {
                name: part.tool.unwrap_or_default(),
                call_id: part.call_id.unwrap_or_default(),
                status,
                title,
            }
        }
        other => PartKind::Other(other.to_string()),
    };
    Ok(PartUpdate {
        session_id: part.session_id,
        message_id: part.message_id,
        part_id: part.id,
        kind,
    })
}

/// `session.status.properties` → [`Event::Status`].
fn parse_status(props: serde_json::Value) -> Result<Event, serde_json::Error> {
    #[derive(Deserialize)]
    struct Props {
        #[serde(rename = "sessionID")]
        session_id: String,
        status: StatusWire,
    }
    #[derive(Deserialize)]
    struct StatusWire {
        #[serde(rename = "type", default)]
        kind: Option<String>,
    }
    let p: Props = serde_json::from_value(props)?;
    let status = match p.status.kind.as_deref() {
        Some("busy") => SessionStatus::Busy,
        Some("idle") | None => SessionStatus::Idle,
        Some(other) => SessionStatus::Other(other.to_string()),
    };
    Ok(Event::Status {
        session_id: p.session_id,
        status,
    })
}

/// `permission.asked.properties` → [`PermissionAsked`].
fn parse_permission(props: serde_json::Value) -> Result<PermissionAsked, serde_json::Error> {
    #[derive(Deserialize)]
    struct Props {
        id: String,
        #[serde(rename = "sessionID")]
        session_id: String,
        permission: String,
        #[serde(default)]
        patterns: Vec<String>,
        #[serde(default)]
        metadata: Metadata,
        #[serde(default)]
        tool: Option<ToolRef>,
    }
    #[derive(Deserialize, Default)]
    struct Metadata {
        #[serde(default)]
        command: Option<String>,
    }
    #[derive(Deserialize)]
    struct ToolRef {
        #[serde(rename = "messageID", default)]
        message_id: Option<String>,
        #[serde(rename = "callID", default)]
        call_id: Option<String>,
    }
    let p: Props = serde_json::from_value(props)?;
    let (message_id, call_id) = match p.tool {
        Some(t) => (t.message_id, t.call_id),
        None => (None, None),
    };
    Ok(PermissionAsked {
        id: p.id,
        session_id: p.session_id,
        permission: p.permission,
        patterns: p.patterns,
        command: p.metadata.command,
        message_id,
        call_id,
    })
}

// ---------------------------------------------------------------------------
// Dedup-by-part-id + backfill reconciliation
// ---------------------------------------------------------------------------

/// A set of part ids already rendered, so a reconnect (which re-emits the
/// in-flight message's parts) does not double-render them. The consumer (#8)
/// marks a part **only once it is fully rendered** (on the part's terminal
/// update / step boundary); an in-flight part is left unseen so a reconnect
/// [`backfill`] re-fetches its full text.
#[derive(Debug, Default, Clone)]
pub struct SeenParts {
    seen: HashSet<String>,
}

impl SeenParts {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record `part_id` as rendered. Returns `true` if it was newly inserted
    /// (i.e. not seen before), `false` if it was already present.
    pub fn mark(&mut self, part_id: &str) -> bool {
        self.seen.insert(part_id.to_string())
    }

    /// Whether `part_id` has already been rendered.
    pub fn contains(&self, part_id: &str) -> bool {
        self.seen.contains(part_id)
    }

    pub fn len(&self) -> usize {
        self.seen.len()
    }

    pub fn is_empty(&self) -> bool {
        self.seen.is_empty()
    }
}

/// One assistant text part recovered by [`backfill`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MissedText {
    pub part_id: String,
    pub text: String,
}

/// Reconcile missed streaming deltas after a reconnect.
///
/// Fetches `GET /session/:id/message` (the authoritative message list) and
/// returns the visible **assistant** text parts whose id is not yet in `seen`,
/// marking each as it goes. Parts already rendered before the disconnect are
/// skipped (dedup by part id); a part that was still in flight at reconnect is
/// returned in full, since the consumer leaves in-flight parts unmarked.
pub async fn backfill(
    client: &OpencodeClient,
    session_id: &str,
    seen: &mut SeenParts,
) -> Result<Vec<MissedText>> {
    let messages = client
        .get_messages(session_id)
        .await
        .context("backfilling missed deltas from GET /session/:id/message")?;

    let mut missed = Vec::new();
    for message in &messages {
        if message.info.role.as_deref() != Some("assistant") {
            continue;
        }
        for (part_id, text) in message.text_parts() {
            // `mark` returns true only for a newly-seen part id.
            if seen.mark(part_id) {
                missed.push(MissedText {
                    part_id: part_id.to_string(),
                    text: text.to_string(),
                });
            }
        }
    }
    Ok(missed)
}

// ---------------------------------------------------------------------------
// Reconnecting subscription
// ---------------------------------------------------------------------------

/// A live, self-reconnecting subscription to an instance's `/global/event`.
///
/// Built on [`reqwest_eventsource::EventSource`] with a constant-delay retry, so
/// [`recv`](Self::recv) is an endless typed-event source: SSE control frames
/// (`Open`), dropped frames (`sync`/heartbeat), and malformed frames are skipped
/// internally, and a dropped connection re-opens transparently.
pub struct Subscription {
    source: EventSource,
}

impl Subscription {
    /// Open a subscription to `{base_url}/global/event` using `http`, retrying
    /// failed connections every `retry`. The GET is issued lazily by the
    /// underlying [`EventSource`]; this constructor only fails if the request
    /// cannot be cloned for retry.
    pub fn connect(http: &reqwest::Client, base_url: &str, retry: Duration) -> Result<Self> {
        let url = format!("{}/global/event", base_url.trim_end_matches('/'));
        let mut source =
            EventSource::new(http.get(url)).context("building /global/event EventSource")?;
        source.set_retry_policy(Box::new(Constant::new(retry, None)));
        Ok(Self { source })
    }

    /// The next surfaced [`Event`], or `None` when the stream is terminally
    /// exhausted (the retry policy gave up). Transparently skips `Open` frames,
    /// dropped/mirror frames, and malformed frames, and reconnects on a dropped
    /// connection (logged at info) — callers just loop on `recv`.
    pub async fn recv(&mut self) -> Option<Event> {
        while let Some(item) = self.source.next().await {
            match item {
                Ok(EsEvent::Open) => continue, // (re)connected — wait for frames.
                Ok(EsEvent::Message(msg)) => match parse_frame(&msg.data) {
                    Ok(Some(event)) => return Some(event),
                    Ok(None) => continue, // sync/heartbeat/mirror — dropped.
                    Err(err) => {
                        tracing::debug!(error = %err, "skipping malformed opencode event frame");
                        continue;
                    }
                },
                Err(reqwest_eventsource::Error::StreamEnded) => {
                    tracing::info!("opencode /global/event stream ended — reconnecting");
                    continue; // Constant retry re-opens on the next poll.
                }
                Err(err) => {
                    tracing::warn!(error = %err, "opencode /global/event error — reconnecting");
                    continue; // Retry policy re-opens, or gives up → next() = None.
                }
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const GATED_GLOBAL: &str = include_str!("../../fixtures/opencode/events/gated-global.sse");
    const GATED: &str = include_str!("../../fixtures/opencode/events/gated.sse");
    const PLAIN: &str = include_str!("../../fixtures/opencode/events/plain.sse");

    /// Parse every `data:` line of an SSE fixture into the surfaced events,
    /// mirroring [`Subscription::recv`]'s frame handling (drop `Ok(None)`,
    /// panic on malformed so a fixture regression is loud).
    fn surfaced(fixture: &str) -> Vec<Event> {
        fixture
            .lines()
            .filter_map(|line| line.strip_prefix("data:").map(str::trim))
            .filter(|data| !data.is_empty())
            .filter_map(|data| parse_frame(data).expect("fixture frame parses"))
            .collect()
    }

    fn deltas(events: &[Event]) -> Vec<Delta> {
        events
            .iter()
            .filter_map(|e| match e {
                Event::Delta(d) => Some(d.clone()),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn drops_sync_and_mirror_frames() {
        // gated-global carries 7 `sync` frames + `*.1` mirrors; none surface.
        let events = surfaced(GATED_GLOBAL);
        assert!(
            !events.iter().any(|e| matches!(e, Event::Other { kind }
                if kind.ends_with(".1") || kind == "sync")),
            "sync/mirror frames must be dropped"
        );
    }

    #[test]
    fn parses_streaming_text_deltas() {
        let ds = deltas(&surfaced(GATED_GLOBAL));
        assert_eq!(ds.len(), 19, "gated-global has 19 text deltas");
        // Reassembled deltas spell the streamed sentence opener.
        let text: String = ds.iter().map(|d| d.delta.as_str()).collect();
        assert!(text.starts_with("The user wants me to run"), "got: {text}");
        assert!(ds.iter().all(|d| d.field == "text"));
    }

    #[test]
    fn parses_tool_lifecycle_from_part_updates() {
        let events = surfaced(GATED_GLOBAL);
        let tools: Vec<(&str, &ToolStatus)> = events
            .iter()
            .filter_map(|e| match e {
                Event::PartUpdated(PartUpdate {
                    kind: PartKind::Tool { name, status, .. },
                    ..
                }) => Some((name.as_str(), status)),
                _ => None,
            })
            .collect();
        assert!(!tools.is_empty(), "expected tool part updates");
        assert!(tools.iter().all(|(name, _)| *name == "bash"));
        assert!(
            tools
                .iter()
                .any(|(_, s)| matches!(s, ToolStatus::Pending | ToolStatus::Running)),
            "expected pending/running tool states"
        );
    }

    #[test]
    fn parses_permission_asked() {
        let events = surfaced(GATED_GLOBAL);
        let perms: Vec<&PermissionAsked> = events
            .iter()
            .filter_map(|e| match e {
                Event::Permission(p) => Some(p),
                _ => None,
            })
            .collect();
        assert_eq!(perms.len(), 1, "one gate in the gated-global fixture");
        let p = perms[0];
        assert!(p.id.starts_with("per_"));
        assert_eq!(p.permission, "bash");
        assert_eq!(p.patterns, vec!["echo hi".to_string()]);
        assert_eq!(p.command.as_deref(), Some("echo hi"));
        assert!(p.call_id.is_some());
    }

    #[test]
    fn parses_session_status_busy() {
        assert!(
            surfaced(GATED_GLOBAL).iter().any(|e| matches!(
                e,
                Event::Status {
                    status: SessionStatus::Busy,
                    ..
                }
            )),
            "expected a busy session.status"
        );
    }

    #[test]
    fn surfaces_connected() {
        assert_eq!(
            surfaced(GATED_GLOBAL)
                .iter()
                .filter(|e| matches!(e, Event::Connected))
                .count(),
            1
        );
    }

    #[test]
    fn directory_scoped_fixture_parses_without_permission() {
        // The directory-scoped `/event` fixture omits `permission.asked` (§10);
        // it must still parse cleanly and yield deltas.
        let events = surfaced(GATED);
        assert!(events.iter().any(|e| matches!(e, Event::Delta(_))));
    }

    #[test]
    fn plain_fixture_parses_and_ignores_noise() {
        // `plain.sse` carries plugin/catalog/reference noise → all `Event::Other`,
        // never a panic, and text still streams.
        let events = surfaced(PLAIN);
        assert!(events.iter().any(|e| matches!(e, Event::Delta(_))));
        assert!(events.iter().any(|e| matches!(e, Event::Other { .. })));
    }

    #[test]
    fn malformed_frame_is_an_error_not_a_panic() {
        assert!(parse_frame("{not json").is_err());
        // A well-formed but mirror frame is dropped, never an error.
        assert_eq!(parse_frame(r#"{"payload":{"type":"sync"}}"#).unwrap(), None);
    }

    #[test]
    fn seen_parts_dedup() {
        let mut seen = SeenParts::new();
        assert!(seen.mark("prt_a"), "first mark is newly-seen");
        assert!(!seen.mark("prt_a"), "second mark is a duplicate");
        assert!(seen.contains("prt_a"));
        assert!(!seen.contains("prt_b"));
        assert_eq!(seen.len(), 1);
    }
}
