//! Whitelist check against persisted `allowed_users` (numeric Telegram IDs);
//! unknown sender → hand off to `pairing`. Enforcement point (opencode runs code).
//! See `docs/design/architecture.md` §5. Issues #4a/#4b.
