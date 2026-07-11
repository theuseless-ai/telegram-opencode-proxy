//! The whitelist enforcement point (A4b).
//!
//! This is the gate — opencode runs code, so an unauthorized sender must never
//! reach a slot. Authorization is now backed by the persisted `allowed_users`
//! table (#4b): a chat is authorized iff `allowed_users` binds it to a slot that
//! is currently live in the runtime registry.
//!
//! The A4a config whitelist (`slot.telegram_id` in `config.toml`) is **not** a
//! separate code path any more — at startup every config slot with a
//! `telegram_id` is idempotently seeded into `allowed_users` (see
//! `telegram::bot::AppState::new`), so config-declared and pairing-approved
//! users flow through the exact same lookup. Pairing (`proxy pair approve`) adds
//! rows here too. An unknown sender resolves to `None`, and the caller hands them
//! to the pairing handshake. See `docs/design/architecture.md` §5. Issue #4b.

use anyhow::Result;

use crate::config::Slot;
use crate::persistence::Db;

/// Resolve the slot a Telegram `chat_id` is authorized for, or `None` if the
/// sender is not on the whitelist (unknown, or bound to a slot that is not
/// currently live).
///
/// Two-step lookup: the persisted `allowed_users` binding (`chat_id -> slot
/// name`), then that name against the passed-in `slots` snapshot of the **runtime
/// registry** (#39), so a binding to a slot that no longer exists matches nobody.
/// Takes the slot slice (rather than the whole registry) so the caller controls
/// the lock discipline — it snapshots under a short read guard and drops it
/// before calling here.
pub fn resolve(db: &Db, slots: &[Slot], chat_id: i64) -> Result<Option<Slot>> {
    let Some(slot_name) = db.allowed_slot(chat_id)? else {
        return Ok(None);
    };
    Ok(slots.iter().find(|slot| slot.name == slot_name).cloned())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn slots_with(names: &[&str]) -> Vec<Slot> {
        names
            .iter()
            .map(|name| Slot {
                name: (*name).to_string(),
                opencode_url: format!("http://127.0.0.1:0/{name}"),
                workdir: ".".into(),
                telegram_id: None,
            })
            .collect()
    }

    fn db() -> Db {
        Db::open_in_memory().expect("in-memory db opens")
    }

    #[test]
    fn resolves_a_bound_and_live_slot() {
        let db = db();
        db.add_allowed(111, "you").unwrap();
        db.add_allowed(222, "wife").unwrap();
        let slots = slots_with(&["you", "wife"]);

        assert_eq!(
            resolve(&db, &slots, 111).unwrap().map(|s| s.name),
            Some("you".to_string())
        );
        assert_eq!(
            resolve(&db, &slots, 222).unwrap().map(|s| s.name),
            Some("wife".to_string())
        );
    }

    #[test]
    fn unknown_sender_is_none() {
        let db = db();
        db.add_allowed(111, "you").unwrap();
        let slots = slots_with(&["you"]);
        assert!(resolve(&db, &slots, 999).unwrap().is_none());
    }

    #[test]
    fn binding_to_a_dead_slot_matches_nobody() {
        // The chat is whitelisted, but its slot is not in the live registry.
        let db = db();
        db.add_allowed(111, "gone").unwrap();
        let slots = slots_with(&["you"]);
        assert!(resolve(&db, &slots, 111).unwrap().is_none());
    }
}
