//! telegram-opencode-proxy — bridge a Telegram bot to `opencode serve`.
//!
//! The crate is built as **both** a library and a binary. `src/main.rs` is a
//! thin CLI shell; all logic lives here so the integration harness (issue #24,
//! `tests/`) can drive the real modules — `AppState`, the dispatcher handlers,
//! the opencode client, and the `serve` bring-up validation — against the
//! in-process mocks. Module layout follows `docs/design/architecture.md` §4.

pub mod admin;
pub mod auth;
pub mod config;
pub mod opencode;
pub mod outbox;
pub mod pairing;
pub mod permission;
pub mod persistence;
pub mod session;
pub mod state;
pub mod telegram;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};

use config::{Config, Model, Slot};
use opencode::client::{self, OpencodeClient};
use opencode::health;
use state::SlotConn;

/// Bring up the daemon: for each slot, connect to its (externally-managed)
/// opencode instance, wait until it's reachable, validate the configured
/// provider/model against its live catalogue, build a client — then run the
/// Telegram dispatcher until Ctrl-C. The proxy is **connect-only**: it does not
/// spawn opencode (start it via `./dev.sh` / systemd / compose).
pub async fn serve(cfg: Config) -> Result<()> {
    tracing::info!(
        slots = cfg.slots.len(),
        provider = %cfg.model.provider_id,
        model = %cfg.model.model_id,
        gated = cfg.permissions.ask.len(),
        "starting proxy (connect-only)"
    );

    // Open persistence first: the registry is seeded from config ∪ the persisted
    // `slots` table (#39), so slots added last run via `proxy connect` reconnect.
    let db = persistence::Db::open(&cfg.db_path)
        .with_context(|| format!("opening SQLite store at {}", cfg.db_path.display()))?;
    tracing::info!(db = %cfg.db_path.display(), "opened SQLite store (WAL)");

    let registry = connect_all(&cfg, &db).await?;

    // Capture the admin socket path before `cfg` is moved into `AppState`, so we
    // can both serve on it and clean it up on shutdown.
    let admin_socket = cfg.admin_socket.clone();
    let bot = teloxide::Bot::new(cfg.bot_token.clone());
    let state = telegram::bot::AppState::new(cfg, registry, db);
    // The admin server shares the same `Arc<AppState>` — read-only is enough for
    // #38 (runtime-mutable slots are #39).
    let admin_state: Arc<dyn admin::AdminState> = state.clone();

    tracing::info!("starting Telegram long-poll dispatcher (Ctrl-C to stop)");
    // Run the dispatcher and the admin control socket concurrently. `run` returns
    // on Ctrl-C; `serve_admin` accepts forever, so whichever finishes first
    // (normally the dispatcher) cancels the other via `select!`.
    tokio::select! {
        () = telegram::bot::run(bot, state) => {
            tracing::info!("dispatcher stopped — shutting down admin socket");
        }
        res = admin::serve_admin(admin_state, admin_socket.clone()) => {
            match res {
                Ok(()) => tracing::warn!("admin socket server exited unexpectedly"),
                Err(err) => tracing::error!(error = format!("{err:#}"), "admin socket server failed"),
            }
        }
    }

    // Best-effort unlink so a restart binds cleanly (and no stale socket lingers).
    match std::fs::remove_file(&admin_socket) {
        Ok(()) => tracing::debug!(socket = %admin_socket.display(), "removed admin socket"),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => {
            tracing::warn!(error = %err, socket = %admin_socket.display(), "failed to remove admin socket on shutdown");
        }
    }

    // TODO(#N2): clean shutdown — drain in-flight turns, close SSE/HTTP, flush
    // SQLite. No opencode children to reap (connect-only).
    Ok(())
}

/// Connect to and validate every configured slot's opencode instance, returning
/// a ready [`SlotConn`] per slot (keyed by slot name).
///
/// Factored out of [`serve`] so the harness can exercise the exact bring-up
/// sequence (readiness → provider catalogue → model validation) that gates a
/// live proxy, without also starting the forever-running dispatcher. For the
/// live daemon, [`connect_all`] additionally folds in the persisted slots.
pub async fn connect_slots(cfg: &Config) -> Result<HashMap<String, SlotConn>> {
    let http = reqwest::Client::builder()
        .build()
        .context("building readiness http client")?;

    let mut registry: HashMap<String, SlotConn> = HashMap::with_capacity(cfg.slots.len());
    for slot in &cfg.slots {
        let conn = bring_up_slot(
            &http,
            slot,
            &cfg.model,
            health::READY_ATTEMPTS,
            health::READY_INTERVAL,
        )
        .await?;
        registry.insert(slot.name.clone(), conn);
    }
    Ok(registry)
}

/// Seed the runtime registry from config `[[slots]]` **∪** the persisted `slots`
/// table (#39). Union is by name; a name present in both is one slot and the
/// **config definition wins** (the persisted row is skipped), so config stays
/// the source of truth for declared seats while runtime-added slots (which live
/// only in the DB) are transparently reconnected on restart. Every slot goes
/// through the same readiness → validate → build-client bring-up.
pub async fn connect_all(cfg: &Config, db: &persistence::Db) -> Result<HashMap<String, SlotConn>> {
    let http = reqwest::Client::builder()
        .build()
        .context("building readiness http client")?;

    let mut registry: HashMap<String, SlotConn> = HashMap::new();
    for slot in &cfg.slots {
        let conn = bring_up_slot(
            &http,
            slot,
            &cfg.model,
            health::READY_ATTEMPTS,
            health::READY_INTERVAL,
        )
        .await?;
        registry.insert(slot.name.clone(), conn);
    }
    for slot in db.list_slots().context("loading persisted slots")? {
        if registry.contains_key(&slot.name) {
            tracing::info!(slot = %slot.name, "persisted slot shadowed by a config [[slots]] entry — config wins");
            continue;
        }
        let conn = bring_up_slot(
            &http,
            &slot,
            &cfg.model,
            health::READY_ATTEMPTS,
            health::READY_INTERVAL,
        )
        .await?;
        registry.insert(slot.name.clone(), conn);
    }
    Ok(registry)
}

/// The single-slot bring-up sequence shared by startup and the `connect` admin
/// command: wait until opencode is reachable, fetch its provider catalogue,
/// validate the configured `{provider, model}` against it, and build a client.
///
/// `attempts`/`interval` bound the readiness wait — startup uses the generous
/// 60 s budget ([`health::READY_ATTEMPTS`]); the interactive `connect` command
/// uses a short one so it fails fast on an unreachable slot.
pub(crate) async fn bring_up_slot(
    http: &reqwest::Client,
    slot: &Slot,
    model: &Model,
    attempts: u32,
    interval: Duration,
) -> Result<SlotConn> {
    tracing::info!(slot = %slot.name, url = %slot.opencode_url, "connecting to opencode");
    health::wait_ready(http, &slot.opencode_url, attempts, interval)
        .await
        .with_context(|| format!("opencode for slot '{}' not reachable", slot.name))?;

    let client = OpencodeClient::for_slot(slot)?;
    let providers = client
        .config_providers()
        .await
        .with_context(|| format!("fetching provider catalogue for slot '{}'", slot.name))?;
    client::validate_model(&providers, &model.provider_id, &model.model_id)
        .with_context(|| format!("validating model for slot '{}'", slot.name))?;

    tracing::info!(slot = %slot.name, "connected — provider/model validated");
    Ok(SlotConn {
        slot: slot.clone(),
        client,
    })
}
