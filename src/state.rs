//! Shared `AppState`: routing table (chat_id → session), pending approvals /
//! pending pairings, opencode instance registry, per-user mpsc queues.
//! See `docs/design/architecture.md` §4/§6. Issues #3/#9.
//!
//! # The slot registry (#39)
//!
//! `AppState` (in `telegram::bot`) holds a **runtime-mutable** registry of live
//! slots — `RwLock<HashMap<String, SlotConn>>` keyed by slot name. Each
//! [`SlotConn`] pairs a slot's definition with a ready [`OpencodeClient`]. The
//! `proxy connect` admin command mutates this map at runtime (add / reconnect),
//! and it is seeded at startup from config `[[slots]]` ∪ the persisted `slots`
//! table so runtime-added slots are reconnected on restart.
//!
//! `OpencodeClient` is cheap to `Clone` (an `Arc`-backed reqwest handle), so the
//! turn path takes a short read-lock, clones the client out, and **drops the
//! guard before any `.await`** — never holding the lock across a suspension.

use crate::config::{Model, Slot};
use crate::opencode::client::OpencodeClient;

/// A live slot: its definition plus a connected, provider/model-validated
/// [`OpencodeClient`]. Cheaply cloneable so callers can snapshot one out of the
/// registry under a short lock and release the lock before awaiting.
#[derive(Debug, Clone)]
pub struct SlotConn {
    /// The slot definition (name, opencode URL, workdir, bound telegram id).
    pub slot: Slot,
    /// A ready client bound to `slot.opencode_url`.
    pub client: OpencodeClient,
    /// The effective model selector for this slot, resolved once at connect from
    /// config `[model]` or opencode's default (#74). The turn sends it on
    /// create/prompt. Its `context_window` field is not authoritative here — the
    /// resolved limit lives in [`context_limit`](Self::context_limit).
    pub model: Model,
    /// The active model's context-window size (tokens), resolved once at connect
    /// from opencode's provider catalogue (or the `[model].context_window`
    /// override). Drives the context-usage % footer (#72); `None` when unknown.
    pub context_limit: Option<u64>,
}
