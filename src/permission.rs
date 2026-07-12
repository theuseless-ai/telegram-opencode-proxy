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

use anyhow::Result;
use teloxide::prelude::*;
use teloxide::types::{ChatId, InlineKeyboardButton, InlineKeyboardMarkup};

use crate::opencode::events::PermissionAsked;
use crate::opencode::types::PermissionReply;
use crate::pairing::now_epoch;
use crate::persistence::{Db, PendingApproval};

/// Callback-data namespace so a stray callback can't be mistaken for a gate.
const CALLBACK_PREFIX: &str = "perm";

/// Post the approval prompt for `perm` to `chat_id` and record a
/// [`PendingApproval`] so the button callback can answer it. The gate's
/// permission id doubles as the callback token.
pub async fn prompt(bot: &Bot, db: &Db, chat_id: i64, perm: &PermissionAsked) -> Result<()> {
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
        "🔐 opencode wants to run a `{}` command:\n\n`{what}`",
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
