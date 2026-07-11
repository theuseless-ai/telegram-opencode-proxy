//! TEMPORARY minimal auth gate (A4a).
//!
//! This is the whitelist enforcement point — opencode runs code, so an
//! unauthorized sender must never reach a slot. For the "wire green" milestone
//! the whitelist is just `slot.telegram_id` in `config.toml`: a chat is
//! authorized iff its numeric id equals some slot's `telegram_id`.
//!
//! **Superseded by A4b** (#4b): the real gate checks the persisted
//! `allowed_users` table and hands unknown senders to the pairing handshake
//! (§5). Until that lands, an unknown sender is simply `None` and the caller
//! logs the numeric `chat_id` (a bootstrap aid) and replies "not authorized".
//! See `docs/design/architecture.md` §5. Issues #4a/#4b.

use crate::config::Slot;

/// Resolve the slot a Telegram `chat_id` is authorized for, or `None` if the
/// sender is not on the whitelist. Matches `slot.telegram_id == Some(chat_id)`.
///
/// Takes a slot slice rather than the whole `Config` so the caller can pass a
/// snapshot of the **runtime registry** (#39) — slots added at runtime via
/// `proxy connect` are authorized here too, not just config `[[slots]]`.
pub fn resolve(slots: &[Slot], chat_id: i64) -> Option<&Slot> {
    slots.iter().find(|slot| slot.telegram_id == Some(chat_id))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn slots_with(specs: &[(&str, Option<i64>)]) -> Vec<Slot> {
        specs
            .iter()
            .map(|(name, id)| Slot {
                name: (*name).to_string(),
                opencode_url: format!("http://127.0.0.1:0/{name}"),
                workdir: ".".into(),
                telegram_id: *id,
            })
            .collect()
    }

    #[test]
    fn resolves_matching_slot() {
        let slots = slots_with(&[("you", Some(111)), ("wife", Some(222))]);
        assert_eq!(resolve(&slots, 111).map(|s| s.name.as_str()), Some("you"));
        assert_eq!(resolve(&slots, 222).map(|s| s.name.as_str()), Some("wife"));
    }

    #[test]
    fn unknown_sender_is_none() {
        let slots = slots_with(&[("you", Some(111))]);
        assert!(resolve(&slots, 999).is_none());
    }

    #[test]
    fn unpaired_slot_matches_nobody() {
        // A slot with no telegram_id must never match, including a 0 chat_id.
        let slots = slots_with(&[("you", None)]);
        assert!(resolve(&slots, 0).is_none());
        assert!(resolve(&slots, 111).is_none());
    }
}
