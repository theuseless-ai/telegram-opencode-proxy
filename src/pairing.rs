//! Confirmation-nonce enrollment: single-use 6-digit codes (TTL, rate-limited),
//! admin Unix socket (0600), `proxy pair list|approve <code> --slot|deny`.
//! See `docs/design/architecture.md` §5. Issue #4b.
//!
//! # The handshake
//!
//! An unknown sender is *not* handed a bearer token. Instead the bot mints a
//! single-use **confirmation nonce** — a random 6-digit code — bound to the
//! sender's `chat_id`, and stores it in `pending_pairings` with a short TTL. The
//! user reads the code back to the admin **out-of-band** (in person / SMS); the
//! admin approves it over the local, `0600` admin socket (`proxy pair approve
//! <code> --slot <name>`), which binds `chat_id → slot` in `allowed_users`.
//! Approval requires shell access to the box, so a leaked code is useless.
//!
//! # Rate limiting
//!
//! Generation is idempotent per `chat_id`: [`issue_code`] first drops any code
//! previously issued to that chat, so a sender spamming the bot only ever holds
//! **one** active row. The freshly minted code replaces the old one.
//!
//! # Time
//!
//! Like [`crate::persistence`], expiry logic is kept unit-testable by taking
//! `now` (epoch seconds) as a parameter rather than reading the clock inside the
//! pure helpers. [`now_epoch`] is the edge that reads the wall clock.

use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Result, anyhow, bail};

use crate::persistence::{Db, PendingPairing};

/// How many times [`issue_code`] retries on a code collision before giving up.
/// The space is 900k codes for at most a couple of users, so a single try
/// nearly always wins; the bound only guards a pathological run.
const CODE_GEN_ATTEMPTS: u32 = 16;

/// The outcome of a successful [`approve`]: enough for the caller to notify the
/// now-paired user and to report the binding back over the admin socket.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApproveOutcome {
    /// The Telegram chat that was just authorized.
    pub chat_id: i64,
    /// The sender's `@username` at request time, if Telegram exposed one.
    pub username: Option<String>,
    /// The slot the chat is now bound to.
    pub slot: String,
}

/// Current wall-clock time in epoch seconds, saturating at 0 before 1970. The
/// single edge that reads the clock; the pure helpers take `now` explicitly.
pub fn now_epoch() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// A random 6-digit code as a zero-padded string (`"000000"`..="999999").
fn random_code() -> String {
    format!("{:06}", rand::random::<u32>() % 1_000_000)
}

/// Issue a fresh pairing code to `chat_id`, replacing any code previously issued
/// to that chat (rate-limit / idempotency: one active code per chat). The code
/// expires `ttl_secs` after `now`.
///
/// Returns the 6-digit code to show the user. `ttl_secs` comes from
/// `[pairing].code_ttl_secs`; `now` is [`now_epoch`] at the call site.
pub fn issue_code(
    db: &Db,
    chat_id: i64,
    username: Option<&str>,
    ttl_secs: i64,
    now: i64,
) -> Result<String> {
    // Drop the sender's prior code first, so spamming never fills the table.
    db.delete_pairings_for_chat(chat_id)?;

    // Pick a code not currently in use by another chat (upsert is by `code`, so
    // a collision would otherwise clobber someone else's pending request).
    let mut code = random_code();
    let mut attempts = 0;
    while db.pairing_by_code(&code)?.is_some() {
        attempts += 1;
        if attempts >= CODE_GEN_ATTEMPTS {
            bail!("could not generate a free pairing code after {CODE_GEN_ATTEMPTS} attempts");
        }
        code = random_code();
    }

    db.insert_pairing(&PendingPairing {
        code: code.clone(),
        chat_id,
        username: username.map(str::to_owned),
        expires_at: now.saturating_add(ttl_secs),
    })?;
    Ok(code)
}

/// Approve a pending code and bind its chat to `slot`.
///
/// Verifies the code exists and has not expired (`expires_at > now`), that
/// `slot` is a real slot (checked against `slot_names` — the live registry), then
/// writes `allowed_users(chat_id → slot)` and consumes the pending row. Returns
/// the bound chat so the caller can notify the user.
pub fn approve(
    db: &Db,
    slot_names: &[String],
    code: &str,
    slot: &str,
    now: i64,
) -> Result<ApproveOutcome> {
    let pairing = db
        .pairing_by_code(code)?
        .ok_or_else(|| anyhow!("no pending pairing with code {code}"))?;

    if pairing.expires_at <= now {
        // Consume the dead row so a stale code can't linger.
        db.delete_pairing(code)?;
        bail!("pairing code {code} has expired — ask the user to message the bot again");
    }

    if !slot_names.iter().any(|name| name == slot) {
        bail!("no such slot '{slot}' — run `proxy slots` to see the available seats");
    }

    db.add_allowed(pairing.chat_id, slot)?;
    db.delete_pairing(code)?;

    Ok(ApproveOutcome {
        chat_id: pairing.chat_id,
        username: pairing.username,
        slot: slot.to_string(),
    })
}

/// Deny (drop) a pending code. Returns `true` if a row was removed, `false` if
/// the code was unknown (already consumed/expired).
pub fn deny(db: &Db, code: &str) -> Result<bool> {
    let existed = db.pairing_by_code(code)?.is_some();
    db.delete_pairing(code)?;
    Ok(existed)
}

/// List the live pending pairings, purging any expired at or before `now` first
/// so the admin only ever sees actionable requests.
pub fn list(db: &Db, now: i64) -> Result<Vec<PendingPairing>> {
    db.purge_expired_pairings(now)?;
    db.list_pairings()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn db() -> Db {
        Db::open_in_memory().expect("in-memory db opens")
    }

    fn is_six_digits(code: &str) -> bool {
        code.len() == 6 && code.chars().all(|c| c.is_ascii_digit())
    }

    #[test]
    fn issue_produces_a_six_digit_code_and_a_pending_row() {
        let db = db();
        let code = issue_code(&db, 42, Some("alice"), 600, 1_000).unwrap();
        assert!(is_six_digits(&code), "code must be 6 digits, got {code:?}");

        let row = db.pairing_by_code(&code).unwrap().expect("row present");
        assert_eq!(row.chat_id, 42);
        assert_eq!(row.username.as_deref(), Some("alice"));
        assert_eq!(row.expires_at, 1_600);
    }

    #[test]
    fn reissue_replaces_the_prior_code_for_the_same_chat() {
        let db = db();
        let first = issue_code(&db, 42, None, 600, 1_000).unwrap();
        let second = issue_code(&db, 42, None, 600, 2_000).unwrap();

        // Only one row survives for the chat: the old code is gone.
        assert!(db.pairing_by_code(&first).unwrap().is_none() || first == second);
        assert!(db.pairing_by_code(&second).unwrap().is_some());
        let all = db.list_pairings().unwrap();
        assert_eq!(all.len(), 1, "one active code per chat, got {all:?}");
        assert_eq!(all[0].chat_id, 42);
    }

    #[test]
    fn approve_binds_allowed_users_and_consumes_the_pending_row() {
        let db = db();
        let code = issue_code(&db, 42, Some("alice"), 600, 1_000).unwrap();

        let names = vec!["you".to_string(), "wife".to_string()];
        let outcome = approve(&db, &names, &code, "wife", 1_100).unwrap();
        assert_eq!(outcome.chat_id, 42);
        assert_eq!(outcome.slot, "wife");
        assert_eq!(outcome.username.as_deref(), Some("alice"));

        // Bound in allowed_users, and the pending row is gone (single-use).
        assert_eq!(db.allowed_slot(42).unwrap().as_deref(), Some("wife"));
        assert!(db.pairing_by_code(&code).unwrap().is_none());
    }

    #[test]
    fn approve_rejects_unknown_code() {
        let db = db();
        let names = vec!["you".to_string()];
        let err = approve(&db, &names, "000000", "you", 1_000).unwrap_err();
        assert!(
            format!("{err}").contains("no pending pairing"),
            "got: {err}"
        );
    }

    #[test]
    fn approve_rejects_expired_code_and_purges_it() {
        let db = db();
        let code = issue_code(&db, 42, None, 600, 1_000).unwrap();
        let names = vec!["you".to_string()];
        // now == expires_at (1_600) → expired (boundary is inclusive).
        let err = approve(&db, &names, &code, "you", 1_600).unwrap_err();
        assert!(format!("{err}").contains("expired"), "got: {err}");
        // The dead row was consumed, and nobody was bound.
        assert!(db.pairing_by_code(&code).unwrap().is_none());
        assert_eq!(db.allowed_slot(42).unwrap(), None);
    }

    #[test]
    fn approve_rejects_unknown_slot() {
        let db = db();
        let code = issue_code(&db, 42, None, 600, 1_000).unwrap();
        let names = vec!["you".to_string()];
        let err = approve(&db, &names, &code, "ghost", 1_100).unwrap_err();
        assert!(format!("{err}").contains("no such slot"), "got: {err}");
        // The code is untouched (still approvable against a real slot) and no bind.
        assert!(db.pairing_by_code(&code).unwrap().is_some());
        assert_eq!(db.allowed_slot(42).unwrap(), None);
    }

    #[test]
    fn deny_removes_a_pending_code() {
        let db = db();
        let code = issue_code(&db, 42, None, 600, 1_000).unwrap();
        assert!(deny(&db, &code).unwrap(), "deny reports it removed a row");
        assert!(db.pairing_by_code(&code).unwrap().is_none());
        // Denying again is a no-op that reports nothing was removed.
        assert!(!deny(&db, &code).unwrap());
    }

    #[test]
    fn list_purges_expired_and_returns_live() {
        let db = db();
        let live = issue_code(&db, 1, None, 600, 1_000).unwrap(); // expires 1_600
        let _stale = issue_code(&db, 2, None, 600, 100).unwrap(); // expires 700

        // now = 1_000 purges chat 2's stale code, keeps chat 1's live one.
        let pending = list(&db, 1_000).unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].code, live);
        assert_eq!(pending[0].chat_id, 1);
    }
}
