//! SQLite (rusqlite, WAL): `routing`, `allowed_users`, `pending_pairings`,
//! `pending_approvals` — survive restart. Migrations + typed accessors.
//! See `docs/design/architecture.md` §4/§5. Issue #3.
//!
//! # Concurrency
//!
//! rusqlite's [`Connection`] is `Send` but **not** `Sync`, so the shared [`Db`]
//! handle wraps it in `Arc<Mutex<Connection>>`. A plain [`std::sync::Mutex`] is
//! enough at our scale (two users, short local queries) — every accessor takes
//! the lock, runs a single statement, and drops it *before* returning. Callers
//! must never hold a guard across an `.await`; the accessors here are all
//! synchronous and finish before control returns to the async caller, so the
//! `chat_id → session_id` read/write in `run_turn` straddles the opencode
//! round-trip without ever pinning the lock.
//!
//! The queries block the async runtime for the (sub-millisecond) duration of a
//! local SQLite call. At this scale that is deliberate: no `spawn_blocking`, no
//! connection pool. If contention ever matters, wrap calls in `spawn_blocking`
//! or move to a pool — the [`Db`] surface stays the same.
//!
//! # Time
//!
//! Expiry logic is kept unit-testable by passing `now` (epoch seconds) into
//! [`Db::purge_expired_pairings`] rather than reading the clock internally.
//! Bookkeeping stamps (`allowed_users.added_at`, `pending_approvals.created_at`)
//! are taken from [`SystemTime`] at the edge, since nothing branches on them.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Result, anyhow};
use rusqlite::{Connection, OptionalExtension, params};

use crate::config::Slot;

/// Current schema version, tracked via SQLite's `PRAGMA user_version`. Bump this
/// and add a matching arm in [`migrate`] when the schema changes.
///
/// v2 adds the `slots` table (#39) so slots added at runtime via `proxy connect`
/// survive a restart. The migration is purely additive.
const SCHEMA_VERSION: i64 = 2;

/// How long a query waits on a locked database before erroring (WAL still
/// serialises writers). Generous — local, low-contention, two users.
const BUSY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// A cheaply-cloneable handle to the proxy's SQLite store.
///
/// `Clone` is an `Arc` bump: every clone shares one [`Connection`] behind one
/// [`Mutex`], so `AppState` and (later) the admin-socket handlers all talk to
/// the same database. `Send + Sync` via the `Arc<Mutex<…>>`.
#[derive(Clone)]
pub struct Db {
    conn: Arc<Mutex<Connection>>,
}

/// A row of `pending_pairings` — an unapproved enrolment request (#4b).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingPairing {
    /// The single-use 6-digit code shown to the user.
    pub code: String,
    /// The Telegram chat the code was issued to.
    pub chat_id: i64,
    /// The sender's `@username`, if Telegram exposed one.
    pub username: Option<String>,
    /// Epoch-seconds expiry; past this the row is purgeable.
    pub expires_at: i64,
}

/// A row of `pending_approvals` — a permission gate awaiting a button tap (#13).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingApproval {
    /// Opaque token embedded in the inline-keyboard callback data.
    pub token: String,
    /// The chat that must answer the gate.
    pub chat_id: i64,
    /// The opencode session the gate belongs to.
    pub session_id: String,
    /// opencode's permission id, used to POST the reply back.
    pub permission_id: Option<String>,
    /// Epoch-seconds creation stamp.
    pub created_at: i64,
}

impl Db {
    /// Open (creating if absent) the database at `path`, enable WAL, set the
    /// busy timeout, and run migrations.
    pub fn open(path: &Path) -> Result<Self> {
        let conn = Connection::open(path)?;
        Self::from_connection(conn)
    }

    /// Open a private in-memory database (tests). WAL is a no-op for `:memory:`,
    /// but the schema and accessors are identical.
    pub fn open_in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        Self::from_connection(conn)
    }

    /// Shared setup: pragmas + migrations, then wrap the connection.
    fn from_connection(conn: Connection) -> Result<Self> {
        // `execute_batch` tolerates the row `PRAGMA journal_mode=WAL` returns.
        conn.execute_batch("PRAGMA journal_mode=WAL;")?;
        conn.busy_timeout(BUSY_TIMEOUT)?;
        migrate(&conn)?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    /// Run `f` with the locked connection. Centralises poison handling so no
    /// accessor has to unwrap a `PoisonError`.
    fn with_conn<T>(&self, f: impl FnOnce(&Connection) -> Result<T>) -> Result<T> {
        let guard = self
            .conn
            .lock()
            .map_err(|_| anyhow!("persistence mutex poisoned"))?;
        f(&guard)
    }

    // --- routing: chat_id → opencode session -----------------------------

    /// The session id currently routed for `chat_id`, if any.
    pub fn get_session(&self, chat_id: i64) -> Result<Option<String>> {
        self.with_conn(|c| {
            let out = c
                .query_row(
                    "SELECT session_id FROM routing WHERE chat_id = ?1",
                    params![chat_id],
                    |row| row.get(0),
                )
                .optional()?;
            Ok(out)
        })
    }

    /// Route `chat_id` to `session_id`, replacing any prior mapping.
    pub fn set_session(&self, chat_id: i64, session_id: &str) -> Result<()> {
        self.with_conn(|c| {
            c.execute(
                "INSERT INTO routing(chat_id, session_id) VALUES(?1, ?2)
                 ON CONFLICT(chat_id) DO UPDATE SET session_id = excluded.session_id",
                params![chat_id, session_id],
            )?;
            Ok(())
        })
    }

    /// Drop the routing entry for `chat_id` (e.g. `/new`). No-op if absent.
    pub fn clear_session(&self, chat_id: i64) -> Result<()> {
        self.with_conn(|c| {
            c.execute("DELETE FROM routing WHERE chat_id = ?1", params![chat_id])?;
            Ok(())
        })
    }

    // --- allowed_users: the persisted whitelist (#4b) --------------------

    /// Add (or re-slot) `chat_id` in the whitelist, stamping `added_at` now.
    pub fn add_allowed(&self, chat_id: i64, slot: &str) -> Result<()> {
        let added_at = now_epoch();
        self.with_conn(|c| {
            c.execute(
                "INSERT INTO allowed_users(chat_id, slot, added_at) VALUES(?1, ?2, ?3)
                 ON CONFLICT(chat_id) DO UPDATE SET slot = excluded.slot, added_at = excluded.added_at",
                params![chat_id, slot, added_at],
            )?;
            Ok(())
        })
    }

    /// The slot `chat_id` is bound to, if it is whitelisted.
    pub fn allowed_slot(&self, chat_id: i64) -> Result<Option<String>> {
        self.with_conn(|c| {
            let out = c
                .query_row(
                    "SELECT slot FROM allowed_users WHERE chat_id = ?1",
                    params![chat_id],
                    |row| row.get(0),
                )
                .optional()?;
            Ok(out)
        })
    }

    /// Every whitelisted `(chat_id, slot)`, ordered by `chat_id`.
    pub fn list_allowed(&self) -> Result<Vec<(i64, String)>> {
        self.with_conn(|c| {
            let mut stmt = c.prepare("SELECT chat_id, slot FROM allowed_users ORDER BY chat_id")?;
            let rows = stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?;
            let mut out = Vec::new();
            for row in rows {
                out.push(row?);
            }
            Ok(out)
        })
    }

    /// Remove `chat_id` from the whitelist. No-op if absent.
    pub fn remove_allowed(&self, chat_id: i64) -> Result<()> {
        self.with_conn(|c| {
            c.execute(
                "DELETE FROM allowed_users WHERE chat_id = ?1",
                params![chat_id],
            )?;
            Ok(())
        })
    }

    // --- pending_pairings: enrolment handshake (#4b) ---------------------

    /// Insert (or replace by `code`) a pending pairing request.
    pub fn insert_pairing(&self, pairing: &PendingPairing) -> Result<()> {
        self.with_conn(|c| {
            c.execute(
                "INSERT INTO pending_pairings(code, chat_id, username, expires_at)
                 VALUES(?1, ?2, ?3, ?4)
                 ON CONFLICT(code) DO UPDATE SET
                     chat_id = excluded.chat_id,
                     username = excluded.username,
                     expires_at = excluded.expires_at",
                params![
                    pairing.code,
                    pairing.chat_id,
                    pairing.username,
                    pairing.expires_at
                ],
            )?;
            Ok(())
        })
    }

    /// Look up a pending pairing by its code.
    pub fn pairing_by_code(&self, code: &str) -> Result<Option<PendingPairing>> {
        self.with_conn(|c| {
            let out = c
                .query_row(
                    "SELECT code, chat_id, username, expires_at
                     FROM pending_pairings WHERE code = ?1",
                    params![code],
                    |row| {
                        Ok(PendingPairing {
                            code: row.get(0)?,
                            chat_id: row.get(1)?,
                            username: row.get(2)?,
                            expires_at: row.get(3)?,
                        })
                    },
                )
                .optional()?;
            Ok(out)
        })
    }

    /// Delete a pending pairing by code (approve/deny consumes it). No-op if absent.
    pub fn delete_pairing(&self, code: &str) -> Result<()> {
        self.with_conn(|c| {
            c.execute(
                "DELETE FROM pending_pairings WHERE code = ?1",
                params![code],
            )?;
            Ok(())
        })
    }

    /// Delete every pairing whose `expires_at` is at or before `now` (epoch
    /// seconds). Returns the number of rows purged.
    pub fn purge_expired_pairings(&self, now: i64) -> Result<usize> {
        self.with_conn(|c| {
            let n = c.execute(
                "DELETE FROM pending_pairings WHERE expires_at <= ?1",
                params![now],
            )?;
            Ok(n)
        })
    }

    // --- pending_approvals: permission gates (#13) -----------------------

    /// Insert (or replace by `token`) a pending approval, stamping `created_at`
    /// now. The passed `created_at` on the struct is ignored on insert.
    pub fn insert_approval(&self, approval: &PendingApproval) -> Result<()> {
        let created_at = now_epoch();
        self.with_conn(|c| {
            c.execute(
                "INSERT INTO pending_approvals(token, chat_id, session_id, permission_id, created_at)
                 VALUES(?1, ?2, ?3, ?4, ?5)
                 ON CONFLICT(token) DO UPDATE SET
                     chat_id = excluded.chat_id,
                     session_id = excluded.session_id,
                     permission_id = excluded.permission_id,
                     created_at = excluded.created_at",
                params![
                    approval.token,
                    approval.chat_id,
                    approval.session_id,
                    approval.permission_id,
                    created_at
                ],
            )?;
            Ok(())
        })
    }

    /// Look up a pending approval by its token.
    pub fn approval(&self, token: &str) -> Result<Option<PendingApproval>> {
        self.with_conn(|c| {
            let out = c
                .query_row(
                    "SELECT token, chat_id, session_id, permission_id, created_at
                     FROM pending_approvals WHERE token = ?1",
                    params![token],
                    |row| {
                        Ok(PendingApproval {
                            token: row.get(0)?,
                            chat_id: row.get(1)?,
                            session_id: row.get(2)?,
                            permission_id: row.get(3)?,
                            created_at: row.get(4)?,
                        })
                    },
                )
                .optional()?;
            Ok(out)
        })
    }

    /// Delete a pending approval by token (once answered). No-op if absent.
    pub fn delete_approval(&self, token: &str) -> Result<()> {
        self.with_conn(|c| {
            c.execute(
                "DELETE FROM pending_approvals WHERE token = ?1",
                params![token],
            )?;
            Ok(())
        })
    }

    /// Every pending approval, ordered oldest-first by `created_at`.
    pub fn list_approvals(&self) -> Result<Vec<PendingApproval>> {
        self.with_conn(|c| {
            let mut stmt = c.prepare(
                "SELECT token, chat_id, session_id, permission_id, created_at
                 FROM pending_approvals ORDER BY created_at, token",
            )?;
            let rows = stmt.query_map([], |row| {
                Ok(PendingApproval {
                    token: row.get(0)?,
                    chat_id: row.get(1)?,
                    session_id: row.get(2)?,
                    permission_id: row.get(3)?,
                    created_at: row.get(4)?,
                })
            })?;
            let mut out = Vec::new();
            for row in rows {
                out.push(row?);
            }
            Ok(out)
        })
    }

    // --- slots: runtime-added seats that must survive restart (#39) -------

    /// Insert (or replace by `name`) a slot. `added_at` is stamped on first
    /// insert and preserved across updates, so re-connecting an existing slot
    /// keeps its original enrolment time.
    pub fn upsert_slot(&self, slot: &Slot) -> Result<()> {
        let added_at = now_epoch();
        let workdir = slot.workdir.to_string_lossy().into_owned();
        self.with_conn(|c| {
            c.execute(
                "INSERT INTO slots(name, opencode_url, workdir, telegram_id, added_at)
                 VALUES(?1, ?2, ?3, ?4, ?5)
                 ON CONFLICT(name) DO UPDATE SET
                     opencode_url = excluded.opencode_url,
                     workdir = excluded.workdir,
                     telegram_id = excluded.telegram_id",
                params![
                    slot.name,
                    slot.opencode_url,
                    workdir,
                    slot.telegram_id,
                    added_at
                ],
            )?;
            Ok(())
        })
    }

    /// Look up a persisted slot by name.
    pub fn get_slot(&self, name: &str) -> Result<Option<Slot>> {
        self.with_conn(|c| {
            let out = c
                .query_row(
                    "SELECT name, opencode_url, workdir, telegram_id
                     FROM slots WHERE name = ?1",
                    params![name],
                    row_to_slot,
                )
                .optional()?;
            Ok(out)
        })
    }

    /// Every persisted slot, in enrolment order (`added_at`, then name).
    pub fn list_slots(&self) -> Result<Vec<Slot>> {
        self.with_conn(|c| {
            let mut stmt = c.prepare(
                "SELECT name, opencode_url, workdir, telegram_id
                 FROM slots ORDER BY added_at, name",
            )?;
            let rows = stmt.query_map([], row_to_slot)?;
            let mut out = Vec::new();
            for row in rows {
                out.push(row?);
            }
            Ok(out)
        })
    }

    /// Remove a persisted slot by name. No-op if absent.
    pub fn remove_slot(&self, name: &str) -> Result<()> {
        self.with_conn(|c| {
            c.execute("DELETE FROM slots WHERE name = ?1", params![name])?;
            Ok(())
        })
    }
}

/// Decode a `slots` row into a [`Slot`]. `workdir` is stored as TEXT and
/// `telegram_id` as a nullable INTEGER.
fn row_to_slot(row: &rusqlite::Row) -> rusqlite::Result<Slot> {
    let workdir: String = row.get(2)?;
    Ok(Slot {
        name: row.get(0)?,
        opencode_url: row.get(1)?,
        workdir: PathBuf::from(workdir),
        telegram_id: row.get(3)?,
    })
}

/// Current wall-clock time in epoch seconds, saturating at 0 before 1970.
fn now_epoch() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Idempotent migration runner. Guarded by `PRAGMA user_version`; every table
/// is `CREATE TABLE IF NOT EXISTS`, so re-running is a no-op.
fn migrate(conn: &Connection) -> Result<()> {
    let version: i64 = conn.query_row("PRAGMA user_version", [], |row| row.get(0))?;
    if version < 1 {
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS routing(
                 chat_id    INTEGER PRIMARY KEY,
                 session_id TEXT NOT NULL
             );
             CREATE TABLE IF NOT EXISTS allowed_users(
                 chat_id  INTEGER PRIMARY KEY,
                 slot     TEXT NOT NULL,
                 added_at INTEGER
             );
             CREATE TABLE IF NOT EXISTS pending_pairings(
                 code       TEXT PRIMARY KEY,
                 chat_id    INTEGER NOT NULL,
                 username   TEXT,
                 expires_at INTEGER NOT NULL
             );
             CREATE TABLE IF NOT EXISTS pending_approvals(
                 token         TEXT PRIMARY KEY,
                 chat_id       INTEGER NOT NULL,
                 session_id    TEXT NOT NULL,
                 permission_id TEXT,
                 created_at    INTEGER
             );",
        )?;
    }
    if version < 2 {
        // Additive: runtime-added slots (#39) persist here so `proxy connect`
        // survives a restart. `telegram_id` is nullable; `workdir` is TEXT.
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS slots(
                 name         TEXT PRIMARY KEY,
                 opencode_url TEXT NOT NULL,
                 workdir      TEXT NOT NULL,
                 telegram_id  INTEGER,
                 added_at     INTEGER
             );",
        )?;
    }
    // Stamp the version regardless (a fresh :memory: db starts at 0).
    conn.pragma_update(None, "user_version", SCHEMA_VERSION)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn db() -> Db {
        Db::open_in_memory().expect("in-memory db opens")
    }

    #[test]
    fn routing_round_trips_and_clears() {
        let db = db();
        assert_eq!(db.get_session(7).unwrap(), None);

        db.set_session(7, "ses_abc").unwrap();
        assert_eq!(db.get_session(7).unwrap().as_deref(), Some("ses_abc"));

        // set replaces, not duplicates.
        db.set_session(7, "ses_def").unwrap();
        assert_eq!(db.get_session(7).unwrap().as_deref(), Some("ses_def"));

        db.clear_session(7).unwrap();
        assert_eq!(db.get_session(7).unwrap(), None);
        // clearing a missing row is a no-op, not an error.
        db.clear_session(7).unwrap();
    }

    #[test]
    fn allowed_users_round_trip() {
        let db = db();
        assert_eq!(db.allowed_slot(1).unwrap(), None);
        assert!(db.list_allowed().unwrap().is_empty());

        db.add_allowed(1, "you").unwrap();
        db.add_allowed(2, "wife").unwrap();
        assert_eq!(db.allowed_slot(1).unwrap().as_deref(), Some("you"));

        // re-adding re-slots rather than duplicating.
        db.add_allowed(1, "admin").unwrap();
        assert_eq!(db.allowed_slot(1).unwrap().as_deref(), Some("admin"));

        let all = db.list_allowed().unwrap();
        assert_eq!(all, vec![(1, "admin".to_string()), (2, "wife".to_string())]);

        db.remove_allowed(1).unwrap();
        assert_eq!(db.allowed_slot(1).unwrap(), None);
        assert_eq!(db.list_allowed().unwrap(), vec![(2, "wife".to_string())]);
    }

    #[test]
    fn pairing_round_trip_and_delete() {
        let db = db();
        let p = PendingPairing {
            code: "123456".to_string(),
            chat_id: 42,
            username: Some("alice".to_string()),
            expires_at: 1_000,
        };
        assert_eq!(db.pairing_by_code("123456").unwrap(), None);

        db.insert_pairing(&p).unwrap();
        assert_eq!(db.pairing_by_code("123456").unwrap().as_ref(), Some(&p));

        db.delete_pairing("123456").unwrap();
        assert_eq!(db.pairing_by_code("123456").unwrap(), None);
    }

    #[test]
    fn pairing_purge_respects_expiry_boundary() {
        let db = db();
        for (code, expires_at) in [("a", 100), ("b", 200), ("c", 300)] {
            db.insert_pairing(&PendingPairing {
                code: code.to_string(),
                chat_id: 1,
                username: None,
                expires_at,
            })
            .unwrap();
        }
        // now = 200 purges `a` (100) and `b` (200, boundary is inclusive) but not `c`.
        let purged = db.purge_expired_pairings(200).unwrap();
        assert_eq!(purged, 2);
        assert!(db.pairing_by_code("a").unwrap().is_none());
        assert!(db.pairing_by_code("b").unwrap().is_none());
        assert!(db.pairing_by_code("c").unwrap().is_some());
    }

    #[test]
    fn approval_round_trip_and_list() {
        let db = db();
        assert_eq!(db.approval("tok1").unwrap(), None);
        assert!(db.list_approvals().unwrap().is_empty());

        let a = PendingApproval {
            token: "tok1".to_string(),
            chat_id: 5,
            session_id: "ses_1".to_string(),
            permission_id: Some("perm_9".to_string()),
            created_at: 0, // ignored on insert; stamped internally.
        };
        db.insert_approval(&a).unwrap();

        let got = db.approval("tok1").unwrap().expect("row present");
        assert_eq!(got.token, "tok1");
        assert_eq!(got.chat_id, 5);
        assert_eq!(got.session_id, "ses_1");
        assert_eq!(got.permission_id.as_deref(), Some("perm_9"));

        db.insert_approval(&PendingApproval {
            token: "tok2".to_string(),
            chat_id: 6,
            session_id: "ses_2".to_string(),
            permission_id: None,
            created_at: 0,
        })
        .unwrap();
        assert_eq!(db.list_approvals().unwrap().len(), 2);

        db.delete_approval("tok1").unwrap();
        assert_eq!(db.approval("tok1").unwrap(), None);
        assert_eq!(db.list_approvals().unwrap().len(), 1);
    }

    fn slot(name: &str, url: &str, telegram_id: Option<i64>) -> Slot {
        Slot {
            name: name.to_string(),
            opencode_url: url.to_string(),
            workdir: PathBuf::from("/tmp/wd"),
            telegram_id,
        }
    }

    #[test]
    fn slots_round_trip_upsert_and_list() {
        let db = db();
        assert_eq!(db.get_slot("you").unwrap(), None);
        assert!(db.list_slots().unwrap().is_empty());

        db.upsert_slot(&slot("you", "http://127.0.0.1:4096", Some(111)))
            .unwrap();
        db.upsert_slot(&slot("wife", "http://127.0.0.1:4097", None))
            .unwrap();

        let got = db.get_slot("you").unwrap().expect("slot present");
        assert_eq!(got.name, "you");
        assert_eq!(got.opencode_url, "http://127.0.0.1:4096");
        assert_eq!(got.workdir, PathBuf::from("/tmp/wd"));
        assert_eq!(got.telegram_id, Some(111));
        assert_eq!(db.get_slot("wife").unwrap().unwrap().telegram_id, None);

        // upsert replaces by name rather than duplicating.
        db.upsert_slot(&slot("you", "http://127.0.0.1:5000", Some(222)))
            .unwrap();
        let got = db.get_slot("you").unwrap().expect("slot present");
        assert_eq!(got.opencode_url, "http://127.0.0.1:5000");
        assert_eq!(got.telegram_id, Some(222));

        let all = db.list_slots().unwrap();
        assert_eq!(all.len(), 2, "upsert must not duplicate");
        let names: Vec<&str> = all.iter().map(|s| s.name.as_str()).collect();
        assert!(names.contains(&"you") && names.contains(&"wife"));

        db.remove_slot("you").unwrap();
        assert_eq!(db.get_slot("you").unwrap(), None);
        assert_eq!(db.list_slots().unwrap().len(), 1);
        // removing a missing slot is a no-op, not an error.
        db.remove_slot("you").unwrap();
    }

    #[test]
    fn slots_survive_reopen_of_same_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("proxy.db");
        {
            let db = Db::open(&path).expect("open fresh db");
            db.upsert_slot(&slot("added", "http://127.0.0.1:9000", Some(7)))
                .unwrap();
        }
        let db = Db::open(&path).expect("reopen db");
        let got = db
            .get_slot("added")
            .unwrap()
            .expect("persisted slot present");
        assert_eq!(got.opencode_url, "http://127.0.0.1:9000");
        assert_eq!(got.telegram_id, Some(7));
    }

    #[test]
    fn migration_is_idempotent() {
        // A single connection migrated twice must not error and keeps schema.
        let conn = Connection::open_in_memory().expect("open");
        migrate(&conn).expect("first migrate");
        migrate(&conn).expect("second migrate is a no-op");
        let version: i64 = conn
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .expect("user_version readable");
        assert_eq!(version, SCHEMA_VERSION);
        // All four tables exist.
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table'
                 AND name IN ('routing','allowed_users','pending_pairings','pending_approvals')",
                [],
                |r| r.get(0),
            )
            .expect("table count");
        assert_eq!(count, 4);
    }

    #[test]
    fn sessions_survive_reopen_of_same_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("proxy.db");

        {
            let db = Db::open(&path).expect("open fresh db");
            db.set_session(101, "ses_persist").unwrap();
            db.add_allowed(101, "you").unwrap();
        } // drop closes the connection.

        // Reopen the same file — migration is idempotent, data is intact.
        let db = Db::open(&path).expect("reopen db");
        assert_eq!(db.get_session(101).unwrap().as_deref(), Some("ses_persist"));
        assert_eq!(db.allowed_slot(101).unwrap().as_deref(), Some("you"));
    }
}
