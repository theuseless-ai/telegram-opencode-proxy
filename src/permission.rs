//! Permission relay (#13): `permission.asked` (on `/global/event`) → Telegram
//! inline buttons → `POST /permission/:id/reply`.
//!
//! When the agent hits an `ask`-gated action, opencode fires `permission.asked`
//! and **blocks the turn**. [`prompt`] posts `[✅ Allow once] [♾️ Always]
//! [❌ Deny]` to the user and records a [`PendingApproval`] keyed by the gate's
//! permission id. A button tap arrives as a `callback_query`; the dispatcher
//! ([`handle_callback`](crate::telegram::bot::handle_callback)) parses it with
//! [`parse_callback`], looks the gate up, and calls `reply_permission` —
//! unblocking the turn. The `[✏️]` revise-with-message variant is a fast-follow.
//! See `docs/design/architecture.md` §2.6/§10.
//!
//! # Which asks belong to a turn (#88)
//!
//! `/global/event` is per-instance, so a turn sees every session's gates — its
//! own, other chats', and those of Task-spawned **subagents**, which run in
//! their own child sessions. A gate names only its own `sessionID`, so matching
//! it against the turn's session id drops subagent gates on the floor: the
//! subagent blocks forever and the delegating `task` call fails. [`TurnScope`]
//! resolves the gate's session up its `parentID` chain instead, so a descendant
//! at any depth maps back to the turn — and so the chat — that owns it.

use std::collections::HashMap;

use anyhow::Result;
use teloxide::prelude::*;
use teloxide::types::{ChatId, InlineKeyboardButton, InlineKeyboardMarkup};

use crate::opencode::client::OpencodeClient;
use crate::opencode::events::PermissionAsked;
use crate::opencode::types::PermissionReply;
use crate::pairing::now_epoch;
use crate::persistence::{Db, PendingApproval};

/// Callback-data namespace so a stray callback can't be mistaken for a gate.
const CALLBACK_PREFIX: &str = "perm";

/// How far up a `parentID` chain to walk before giving up. Real nesting is a
/// handful deep; the cap only bounds the damage if opencode ever hands back a
/// cycle, so the resolver can't spin on the turn's event loop.
const MAX_PARENT_DEPTH: usize = 8;

/// Which session in a turn raised a gate — the answer [`TurnScope`] produces.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Origin {
    /// The turn's own session: the primary agent asked.
    Main,
    /// A Task-spawned descendant asked, named when opencode reports an agent.
    Subagent { agent: Option<String> },
}

/// Resolves whether a `permission.asked` belongs to one turn, memoizing the
/// verdict per session (#88).
///
/// The turn's own session answers without any I/O. Any other session is walked
/// up its `parentID` chain: reaching the turn's session means a subagent of
/// ours asked, and reaching a different root means the gate is another chat's
/// (or another client's) to answer — we must stay silent, or two chats would
/// prompt for one gate and the loser's tap would 404.
///
/// Verdicts are cached because parentage is immutable in opencode, so one
/// lookup per session settles it for the rest of the turn. Failures are *not*
/// cached: a transient error would otherwise permanently silence a live gate.
pub struct TurnScope {
    /// The turn's session id — the root every chain is resolved against.
    root: String,
    /// session id → verdict. `None` marks a foreign session.
    seen: HashMap<String, Option<Origin>>,
}

impl TurnScope {
    /// A scope rooted at this turn's `session_id`.
    pub fn new(session_id: impl Into<String>) -> Self {
        Self {
            root: session_id.into(),
            seen: HashMap::new(),
        }
    }

    /// Resolve `session_id` against this turn: `Some(origin)` when the turn owns
    /// it, `None` when it belongs to someone else or can't be resolved.
    pub async fn resolve(&mut self, client: &OpencodeClient, session_id: &str) -> Option<Origin> {
        if session_id == self.root {
            return Some(Origin::Main);
        }
        if let Some(verdict) = self.seen.get(session_id) {
            return verdict.clone();
        }

        // Walk child → parent, remembering each session's agent so the verdict
        // can name whichever one actually asked.
        let mut chain: Vec<(String, Option<String>)> = Vec::new();
        let mut cursor = session_id.to_string();
        let mut owned = false;
        // Whether the walk reached a conclusive outcome (root match, a proven
        // foreign root, or an already-cached ancestor) before the depth cap.
        // Exhausting the cap without one means we don't actually know — that
        // must NOT be conflated with a proven-foreign verdict below.
        let mut concluded = false;

        for _ in 0..MAX_PARENT_DEPTH {
            let session = match client.get_session(&cursor).await {
                Ok(Some(session)) => session,
                // Unknown to this instance — nothing to attribute it to.
                Ok(None) => {
                    concluded = true;
                    break;
                }
                Err(err) => {
                    // Leave the chain uncached so a later gate retries rather
                    // than inheriting a verdict we never actually established.
                    tracing::warn!(
                        session_id = %cursor,
                        error = %err,
                        "resolving a permission's session failed — not surfacing this gate"
                    );
                    return None;
                }
            };
            chain.push((cursor.clone(), session.agent.clone()));
            match session.parent_id {
                // A root that isn't ours: another chat's turn, or another client.
                None => {
                    concluded = true;
                    break;
                }
                Some(parent) if parent == self.root => {
                    owned = true;
                    concluded = true;
                    break;
                }
                Some(parent) => match self.seen.get(&parent) {
                    // Inherit a verdict already settled for an ancestor.
                    Some(Some(_)) => {
                        owned = true;
                        concluded = true;
                        break;
                    }
                    Some(None) => {
                        concluded = true;
                        break;
                    }
                    None => cursor = parent,
                },
            }
        }

        if !concluded {
            // The chain is deeper than MAX_PARENT_DEPTH (or, in practice, opencode
            // handed back a cycle). Caching these as foreign would be wrong — we
            // never actually proved that — and would poison any shallower
            // ancestor that a later, shorter walk could otherwise resolve.
            tracing::warn!(
                session_id,
                depth = MAX_PARENT_DEPTH,
                "permission session's parentID chain exceeded the depth cap — not surfacing this gate"
            );
            return None;
        }

        // Every session on the walked path shares the root's verdict, so one
        // walk settles a whole branch. Each keeps its own agent name.
        for (id, agent) in chain {
            let verdict = owned.then_some(Origin::Subagent { agent });
            self.seen.insert(id, verdict);
        }
        self.seen.get(session_id).cloned().flatten()
    }
}

/// Post the approval prompt for `perm` to `chat_id` and record a
/// [`PendingApproval`] so the button callback can answer it. The gate's
/// permission id doubles as the callback token. `origin` names the asking
/// subagent, so approving a delegated command is an informed decision rather
/// than one indistinguishable from the primary agent's own (#88).
pub async fn prompt(
    bot: &Bot,
    db: &Db,
    chat_id: i64,
    perm: &PermissionAsked,
    origin: &Origin,
) -> Result<()> {
    db.insert_approval(&PendingApproval {
        token: perm.id.clone(),
        chat_id,
        session_id: perm.session_id.clone(),
        permission_id: Some(perm.id.clone()),
        created_at: now_epoch(),
    })?;

    let what = perm
        .command
        .clone()
        .unwrap_or_else(|| perm.patterns.join(", "));
    let text = format!(
        "🔐 {} wants to run a `{}` command:\n\n`{what}`",
        asker(origin),
        perm.permission
    );
    let keyboard = InlineKeyboardMarkup::new([[
        InlineKeyboardButton::callback(
            "✅ Allow once",
            callback_data(&perm.id, PermissionReply::Once),
        ),
        InlineKeyboardButton::callback(
            "♾️ Always",
            callback_data(&perm.id, PermissionReply::Always),
        ),
        InlineKeyboardButton::callback("❌ Deny", callback_data(&perm.id, PermissionReply::Reject)),
    ]]);
    bot.send_message(ChatId(chat_id), text)
        .reply_markup(keyboard)
        .await?;
    Ok(())
}

/// How the prompt names whoever raised the gate. A subagent is called out by
/// name, since "opencode wants to run" reads as the primary agent and would
/// misrepresent a command the user never directly asked for.
fn asker(origin: &Origin) -> String {
    match origin {
        Origin::Main => "opencode".to_string(),
        Origin::Subagent { agent: Some(name) } => format!("subagent `{name}`"),
        Origin::Subagent { agent: None } => "a subagent".to_string(),
    }
}

/// Encode the callback data for one button: `perm:{token}:{action}`.
fn callback_data(token: &str, reply: PermissionReply) -> String {
    format!("{CALLBACK_PREFIX}:{token}:{}", action_str(reply))
}

fn action_str(reply: PermissionReply) -> &'static str {
    match reply {
        PermissionReply::Once => "once",
        PermissionReply::Always => "always",
        PermissionReply::Reject => "reject",
    }
}

/// Parse `perm:{token}:{action}` callback data → `(token, reply)`, or `None` if
/// it isn't one of our permission callbacks.
pub fn parse_callback(data: &str) -> Option<(String, PermissionReply)> {
    let mut parts = data.splitn(3, ':');
    if parts.next()? != CALLBACK_PREFIX {
        return None;
    }
    let token = parts.next()?.to_string();
    let reply = match parts.next()? {
        "once" => PermissionReply::Once,
        "always" => PermissionReply::Always,
        "reject" => PermissionReply::Reject,
        _ => return None,
    };
    Some((token, reply))
}

/// The confirmation text that replaces the buttons after a decision.
pub fn decision_text(reply: PermissionReply) -> &'static str {
    match reply {
        PermissionReply::Once => "✅ Allowed (once).",
        PermissionReply::Always => "♾️ Always allowed.",
        PermissionReply::Reject => "❌ Denied.",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn callback_data_round_trips() {
        for reply in [
            PermissionReply::Once,
            PermissionReply::Always,
            PermissionReply::Reject,
        ] {
            let data = callback_data("per_abc123", reply);
            assert_eq!(
                parse_callback(&data),
                Some(("per_abc123".to_string(), reply))
            );
        }
    }

    #[test]
    fn parse_callback_rejects_foreign_data() {
        assert!(parse_callback("pair:123:approve").is_none());
        assert!(parse_callback("perm:per_x:frobnicate").is_none());
        assert!(parse_callback("garbage").is_none());
    }
}
