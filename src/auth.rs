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

use crate::config::{Config, Slot};

/// Resolve the slot a Telegram `chat_id` is authorized for, or `None` if the
/// sender is not on the whitelist. Matches `slot.telegram_id == Some(chat_id)`.
pub fn resolve(cfg: &Config, chat_id: i64) -> Option<&Slot> {
    cfg.slots
        .iter()
        .find(|slot| slot.telegram_id == Some(chat_id))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg_with(slots: &[(&str, Option<i64>)]) -> Config {
        let mut toml = String::from(
            "bot_token = \"t\"\nadmin_socket = \"/tmp/a.sock\"\n\
             [model]\nprovider_id = \"p\"\nmodel_id = \"m\"\n",
        );
        for (name, id) in slots {
            toml.push_str(&format!(
                "[[slots]]\nname = \"{name}\"\nopencode_url = \"http://127.0.0.1:0/{name}\"\nworkdir = \".\"\n"
            ));
            if let Some(id) = id {
                toml.push_str(&format!("telegram_id = {id}\n"));
            }
        }
        toml::from_str(&toml).expect("test config parses")
    }

    #[test]
    fn resolves_matching_slot() {
        let cfg = cfg_with(&[("you", Some(111)), ("wife", Some(222))]);
        assert_eq!(resolve(&cfg, 111).map(|s| s.name.as_str()), Some("you"));
        assert_eq!(resolve(&cfg, 222).map(|s| s.name.as_str()), Some("wife"));
    }

    #[test]
    fn unknown_sender_is_none() {
        let cfg = cfg_with(&[("you", Some(111))]);
        assert!(resolve(&cfg, 999).is_none());
    }

    #[test]
    fn unpaired_slot_matches_nobody() {
        // A slot with no telegram_id must never match, including a 0 chat_id.
        let cfg = cfg_with(&[("you", None)]);
        assert!(resolve(&cfg, 0).is_none());
        assert!(resolve(&cfg, 111).is_none());
    }
}
