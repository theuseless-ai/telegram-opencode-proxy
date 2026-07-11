//! Config: TOML file + `clap` CLI (`serve` / `pair`), `[[slots]]`, `[model]`
//! selector, `admin_socket`, validation. The full `[pairing]` block is deferred
//! to #4b. See `docs/design/architecture.md` §11. Issue #2.

use std::collections::HashSet;
use std::ffi::OsString;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow, ensure};
use clap::{Parser, Subcommand};
use serde::Deserialize;
use toml_edit::{ArrayOfTables, DocumentMut, Item, Table, value};

/// CLI entry point: `serve` (daemon) or `pair` (admin enrollment client).
#[derive(Debug, Parser)]
#[command(name = "telegram-opencode-proxy", version, about)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

/// Top-level subcommands.
#[derive(Debug, Subcommand)]
pub enum Command {
    /// Run the proxy daemon.
    Serve {
        /// Path to the TOML config file.
        #[arg(short, long, default_value = "config.toml")]
        config: PathBuf,
    },
    /// Query the running daemon over its admin socket and print slot status.
    Status {
        /// Path to the TOML config file (read for `admin_socket`).
        #[arg(short, long, default_value = "config.toml")]
        config: PathBuf,
        /// Admin socket path override. When set, the config file is not read.
        #[arg(long)]
        socket: Option<PathBuf>,
    },
    /// Idempotently ensure a slot exists and is connected on the running daemon
    /// (#39): reports `connected`, `reconnected`, or `added`.
    Connect {
        /// Slot name to ensure connected.
        name: String,
        /// opencode base URL — required when adding a slot that does not exist.
        #[arg(long)]
        url: Option<String>,
        /// Working directory for a newly-added slot (defaults to ".").
        #[arg(long)]
        workdir: Option<String>,
        /// Telegram id to bind a newly-added slot to.
        #[arg(long)]
        telegram_id: Option<i64>,
        /// Path to the TOML config file (read for `admin_socket`).
        #[arg(short, long, default_value = "config.toml")]
        config: PathBuf,
        /// Admin socket path override. When set, the config file is not read.
        #[arg(long)]
        socket: Option<PathBuf>,
    },
    /// Per-slot inventory over the admin socket (#4b): name, opencode URL,
    /// workdir, bound Telegram ids, reachability, and config-vs-db source. Use it
    /// to pick a `--slot` for `pair approve`.
    Slots {
        /// Path to the TOML config file (read for `admin_socket`).
        #[arg(short, long, default_value = "config.toml")]
        config: PathBuf,
        /// Admin socket path override. When set, the config file is not read.
        #[arg(long)]
        socket: Option<PathBuf>,
    },
    /// Admin enrollment client (#4b): list / approve / deny pending pairings.
    Pair {
        #[command(subcommand)]
        action: PairAction,
        /// Path to the TOML config file (read for `admin_socket`).
        #[arg(short, long, default_value = "config.toml")]
        config: PathBuf,
        /// Admin socket path override. When set, the config file is not read.
        #[arg(long)]
        socket: Option<PathBuf>,
    },
}

/// Admin pairing subcommands (stubbed until #4b).
#[derive(Debug, Subcommand)]
pub enum PairAction {
    /// List pending pairing requests.
    List,
    /// Approve a pending request and bind it to a slot.
    Approve {
        /// The 6-digit code the user was shown.
        code: String,
        /// Slot name to bind the account to.
        #[arg(long)]
        slot: String,
    },
    /// Deny (drop) a pending request.
    Deny {
        /// The 6-digit code to reject.
        code: String,
    },
}

/// Proxy configuration, loaded from `config.toml`.
#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    /// Telegram bot token. The `TELOXIDE_TOKEN` env var overrides this.
    #[serde(default)]
    pub bot_token: String,
    /// Local admin control socket — the CLI ↔ daemon channel (#38). Bound by
    /// `serve` and dialed by `proxy status`; kept local-only, perms 0600.
    pub admin_socket: PathBuf,
    /// User seats. `telegram_id` is bound at pairing time (#4b), not here.
    pub slots: Vec<Slot>,
    /// Model selector — must match a provider/model configured in `opencode.json`.
    pub model: Model,
    /// Session permission rules (see #13).
    #[serde(default)]
    pub permissions: Permissions,
    /// Pairing / enrolment tuning (#4b). Optional — a missing `[pairing]` block
    /// uses the defaults.
    #[serde(default)]
    pub pairing: Pairing,
    /// Path to the proxy's SQLite store (routing + whitelist + pending
    /// pairings/approvals; #3). Relative paths resolve against the process
    /// working directory. Defaults to `proxy.db`. WAL creates sidecar
    /// `-wal`/`-shm` files next to it.
    #[serde(default = "default_db_path")]
    pub db_path: PathBuf,
}

/// Default SQLite path when `db_path` is omitted from config.
fn default_db_path() -> PathBuf {
    PathBuf::from("proxy.db")
}

/// A user seat: one opencode instance bound to one working directory.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct Slot {
    pub name: String,
    pub opencode_url: String,
    pub workdir: PathBuf,
    /// TEMPORARY minimal auth gate (A4a): the numeric Telegram id bound to this
    /// slot. This is a bootstrap stand-in and is **superseded by A4b** pairing,
    /// which binds ids in the SQLite `allowed_users` table (#4b) rather than in
    /// config. Optional so an unpaired slot simply matches nobody.
    #[serde(default)]
    pub telegram_id: Option<i64>,
}

/// Model selector passed to opencode as `{ providerID, modelID }`.
#[derive(Debug, Clone, Deserialize)]
pub struct Model {
    pub provider_id: String,
    pub model_id: String,
}

/// Session permission rules.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct Permissions {
    /// Bash patterns PATCHed onto each session (`deny` until #13 flips to `ask`).
    #[serde(default)]
    pub ask: Vec<String>,
}

/// Pairing / enrolment tuning (#4b). Governs the confirmation-nonce lifecycle.
#[derive(Debug, Clone, Deserialize)]
pub struct Pairing {
    /// How long an issued pairing code stays valid, in seconds. Defaults to
    /// 600 (10 minutes); past this the code is purgeable and `pair approve`
    /// rejects it.
    #[serde(default = "default_code_ttl_secs")]
    pub code_ttl_secs: i64,
}

impl Default for Pairing {
    fn default() -> Self {
        Self {
            code_ttl_secs: default_code_ttl_secs(),
        }
    }
}

/// Default pairing-code TTL (10 minutes) when `[pairing]` omits it.
fn default_code_ttl_secs() -> i64 {
    600
}

impl Config {
    /// Load config from `path`, apply the `TELOXIDE_TOKEN` env override, and validate.
    pub fn load(path: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("reading config {}", path.display()))?;
        let mut cfg: Config =
            toml::from_str(&text).with_context(|| format!("parsing config {}", path.display()))?;
        if let Ok(token) = std::env::var("TELOXIDE_TOKEN") {
            cfg.bot_token = token;
        }
        cfg.validate()?;
        Ok(cfg)
    }

    /// Reject configs that would fail at runtime, with actionable messages.
    fn validate(&self) -> Result<()> {
        ensure!(
            !self.bot_token.is_empty(),
            "bot_token is empty — set it in config or via the TELOXIDE_TOKEN env var"
        );
        ensure!(!self.slots.is_empty(), "no [[slots]] configured");
        ensure!(
            !self.model.provider_id.is_empty() && !self.model.model_id.is_empty(),
            "[model] provider_id and model_id must both be set"
        );
        let mut urls = HashSet::new();
        for slot in &self.slots {
            ensure!(
                urls.insert(slot.opencode_url.as_str()),
                "duplicate opencode_url '{}' — slots must target distinct instances",
                slot.opencode_url
            );
            ensure!(
                slot.workdir.is_dir(),
                "slot '{}' workdir does not exist: {}",
                slot.name,
                slot.workdir.display()
            );
        }
        Ok(())
    }
}

/// Persist `slot` into the `[[slots]]` array-of-tables of the config file at
/// `path`, **format- and comment-preserving** (issue #45). `config.toml` is the
/// single source of truth for slots, so `proxy connect` writes here rather than
/// to the DB.
///
/// If a `[[slots]]` entry with the same `name` already exists, its fields are
/// updated in place; otherwise a new table is appended. The write is atomic-ish:
/// the mutated document is written to a sibling `*.tmp` file and then renamed
/// over the original, so a crash mid-write never leaves a truncated config.
pub fn upsert_slot(path: &Path, slot: &Slot) -> Result<()> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("reading config {} for slot update", path.display()))?;
    let mut doc: DocumentMut = text
        .parse()
        .with_context(|| format!("parsing config {} for slot update", path.display()))?;

    let item = doc
        .as_table_mut()
        .entry("slots")
        .or_insert_with(|| Item::ArrayOfTables(ArrayOfTables::new()));
    let slots = item.as_array_of_tables_mut().ok_or_else(|| {
        anyhow!(
            "`slots` in {} is not a [[slots]] array of tables",
            path.display()
        )
    })?;

    let existing = slots
        .iter()
        .position(|t| t.get("name").and_then(Item::as_str) == Some(slot.name.as_str()));
    match existing {
        Some(idx) => {
            if let Some(table) = slots.get_mut(idx) {
                set_slot_fields(table, slot);
            }
        }
        None => {
            let mut table = Table::new();
            set_slot_fields(&mut table, slot);
            slots.push(table);
        }
    }

    let rendered = doc.to_string();
    let mut tmp: OsString = path.as_os_str().to_owned();
    tmp.push(".tmp");
    let tmp = PathBuf::from(tmp);
    std::fs::write(&tmp, rendered)
        .with_context(|| format!("writing temp config {}", tmp.display()))?;
    std::fs::rename(&tmp, path).with_context(|| {
        format!(
            "renaming temp config {} over {}",
            tmp.display(),
            path.display()
        )
    })?;
    Ok(())
}

/// Write a slot's fields into a `[[slots]]` table. `telegram_id` is only written
/// when present, so an unbound slot leaves the key absent (matching `#[serde]`
/// `Option` semantics on load).
fn set_slot_fields(table: &mut Table, slot: &Slot) {
    table["name"] = value(slot.name.as_str());
    table["opencode_url"] = value(slot.opencode_url.as_str());
    table["workdir"] = value(slot.workdir.to_string_lossy().into_owned());
    if let Some(id) = slot.telegram_id {
        table["telegram_id"] = value(id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Both slots use `.` (exists in the test cwd) but distinct URLs.
    fn sample() -> String {
        r#"
            bot_token = "t"
            admin_socket = "/tmp/admin.sock"
            [[slots]]
            name = "you"
            opencode_url = "http://127.0.0.1:4096"
            workdir = "."
            [[slots]]
            name = "wife"
            opencode_url = "http://127.0.0.1:4097"
            workdir = "."
            [model]
            provider_id = "llm-lan"
            model_id = "Qwen3.6-35B-A3B-bf16"
        "#
        .to_string()
    }

    #[test]
    fn parses_and_validates() {
        let cfg: Config = toml::from_str(&sample()).unwrap();
        cfg.validate().unwrap();
        assert_eq!(cfg.slots.len(), 2);
        assert_eq!(cfg.model.provider_id, "llm-lan");
    }

    #[test]
    fn rejects_duplicate_urls() {
        let cfg: Config = toml::from_str(&sample().replace("4097", "4096")).unwrap();
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn rejects_empty_model() {
        let cfg: Config = toml::from_str(&sample().replace("llm-lan", "")).unwrap();
        assert!(cfg.validate().is_err());
    }

    fn slot(name: &str, url: &str, telegram_id: Option<i64>) -> Slot {
        Slot {
            name: name.to_string(),
            opencode_url: url.to_string(),
            workdir: PathBuf::from("/srv/wd"),
            telegram_id,
        }
    }

    #[test]
    fn upsert_slot_appends_and_preserves_comments() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("config.toml");
        let original = "\
# top comment — keep me
bot_token = \"t\"
admin_socket = \"/tmp/admin.sock\"

[[slots]]
name = \"you\" # inline keep-me
opencode_url = \"http://127.0.0.1:4096\"
workdir = \".\"

[model]
provider_id = \"llm-lan\"
model_id = \"Qwen3.6-35B-A3B-bf16\"
";
        std::fs::write(&path, original).unwrap();

        upsert_slot(&path, &slot("wife", "http://127.0.0.1:4097", Some(222))).unwrap();

        let back = std::fs::read_to_string(&path).unwrap();
        // Comments survive the round-trip.
        assert!(back.contains("# top comment — keep me"));
        assert!(back.contains("# inline keep-me"));
        // The new slot is parseable as a real Config slot.
        let cfg: Config = toml::from_str(&back).unwrap();
        assert_eq!(cfg.slots.len(), 2);
        let added = cfg.slots.iter().find(|s| s.name == "wife").unwrap();
        assert_eq!(added.opencode_url, "http://127.0.0.1:4097");
        assert_eq!(added.workdir, PathBuf::from("/srv/wd"));
        assert_eq!(added.telegram_id, Some(222));
    }

    #[test]
    fn upsert_slot_updates_in_place_by_name() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("config.toml");
        std::fs::write(
            &path,
            "\
admin_socket = \"/tmp/a.sock\"
[[slots]]
name = \"you\"
opencode_url = \"http://127.0.0.1:4096\"
workdir = \".\"
[model]
provider_id = \"llm-lan\"
model_id = \"m\"
",
        )
        .unwrap();

        upsert_slot(&path, &slot("you", "http://127.0.0.1:5000", Some(999))).unwrap();

        let cfg: Config = toml::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
        assert_eq!(cfg.slots.len(), 1, "update must not duplicate the entry");
        assert_eq!(cfg.slots[0].opencode_url, "http://127.0.0.1:5000");
        assert_eq!(cfg.slots[0].telegram_id, Some(999));
    }
}
