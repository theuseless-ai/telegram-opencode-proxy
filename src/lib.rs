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
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};

use config::{Config, Model, Slot};
use opencode::client::{self, OpencodeClient};
use opencode::health;
use state::SlotConn;

/// How long graceful shutdown waits for in-flight turns to finish before giving
/// up (#21). Generous — a turn can span a full model generation — but well under
/// systemd's default `TimeoutStopSec` (90s).
const SHUTDOWN_GRACE: Duration = Duration::from_secs(30);

/// Bring up the daemon: for each slot, connect to its (externally-managed)
/// opencode instance, wait until it's reachable, validate the configured
/// provider/model against its live catalogue, build a client — then run the
/// Telegram dispatcher until Ctrl-C. The proxy is **connect-only**: it does not
/// spawn opencode (start it via `./dev.sh` / systemd / compose).
///
/// `config_path` is the file `cfg` was loaded from; the daemon writes
/// `proxy connect`-added slots back to it (#45), so config is the single source
/// of truth for slots.
pub async fn serve(cfg: Config, config_path: PathBuf) -> Result<()> {
    tracing::info!(
        slots = cfg.slots.len(),
        provider = %cfg.model.provider_id,
        model = %cfg.model.model_id,
        gated = cfg.permissions.ask.len(),
        "starting proxy (connect-only)"
    );

    // Open persistence for routing + whitelist + pending pairings/approvals (#3).
    let db = persistence::Db::open(&cfg.db_path)
        .with_context(|| format!("opening SQLite store at {}", cfg.db_path.display()))?;
    tracing::info!(db = %cfg.db_path.display(), "opened SQLite store (WAL)");

    // Capture the admin socket path before `cfg` is moved into `AppState`, so we
    // can both serve on it and clean it up on shutdown.
    let admin_socket = cfg.admin_socket.clone();
    let bot = teloxide::Bot::new(cfg.bot_token.expose());
    // Start with an EMPTY registry and bring the configured slots up
    // **concurrently, in the background** (#51). The dispatcher comes online
    // immediately instead of waiting out each slot's readiness budget, so one
    // unreachable opencode never delays replies for the healthy slots. The
    // whitelist is seeded from `cfg.slots` inside `AppState::new`, independent of
    // connection state, so bound users are authorized right away.
    let state = telegram::bot::AppState::new(cfg, config_path, HashMap::new(), db, bot.clone());
    spawn_slot_bringup(
        Arc::clone(&state),
        health::READY_ATTEMPTS,
        health::READY_INTERVAL,
    );
    // The admin server shares the same `Arc<AppState>` — read-only is enough for
    // #38 (runtime-mutable slots are #39).
    let admin_state: Arc<dyn admin::AdminState> = state.clone();

    // The admin control socket is a SECONDARY feature — run it in a background
    // task so a bind/permission failure only logs and is tolerated, never taking
    // down the bot. The dispatcher is the primary and blocks until Ctrl-C.
    let admin_socket_task = admin_socket.clone();
    let admin_task = tokio::spawn(async move {
        if let Err(err) = admin::serve_admin(admin_state, admin_socket_task).await {
            tracing::error!(
                error = format!("{err:#}"),
                "admin control socket unavailable — continuing without it"
            );
        }
    });

    tracing::info!("starting Telegram long-poll dispatcher (Ctrl-C / SIGTERM to stop)");
    telegram::bot::run(bot, state.clone()).await;
    tracing::info!("dispatcher stopped — shutting down gracefully");

    // Graceful shutdown (#21): let in-flight per-user turns finish (bounded),
    // then flush SQLite. Connect-only, so there are no opencode children to reap.
    state.shutdown(SHUTDOWN_GRACE).await;

    // Stop the admin socket task and unlink the socket so a restart binds cleanly.
    admin_task.abort();
    match std::fs::remove_file(&admin_socket) {
        Ok(()) => tracing::debug!(socket = %admin_socket.display(), "removed admin socket"),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => {
            tracing::warn!(error = %err, socket = %admin_socket.display(), "failed to remove admin socket on shutdown");
        }
    }

    tracing::info!("shutdown complete");
    Ok(())
}

/// Connect to and validate every configured slot's opencode instance
/// **synchronously**, returning a ready [`SlotConn`] per slot (keyed by slot
/// name) once all have resolved.
///
/// `config.toml` is the single source of truth for slots (#45), so the registry
/// is built from `cfg.slots` alone. [`serve`] does **not** use this — it brings
/// slots up in the background via [`spawn_slot_bringup`] so the dispatcher never
/// blocks on readiness (#51). This blocking, fully-resolved variant is what the
/// hermetic harness drives to exercise the exact bring-up sequence (readiness →
/// provider catalogue → model validation) without the forever-running dispatcher.
pub async fn connect_slots(cfg: &Config) -> Result<HashMap<String, SlotConn>> {
    let http = reqwest::Client::builder()
        .build()
        .context("building readiness http client")?;

    let mut registry: HashMap<String, SlotConn> = HashMap::with_capacity(cfg.slots.len());
    for slot in &cfg.slots {
        match bring_up_slot(
            &http,
            slot,
            &cfg.model,
            health::READY_ATTEMPTS,
            health::READY_INTERVAL,
        )
        .await
        {
            Ok(conn) => {
                registry.insert(slot.name.clone(), conn);
            }
            // Best-effort: one unreachable / mis-provisioned slot must NOT crash
            // the daemon. Log it and keep serving the slots that did connect; the
            // skipped one comes up later (no restart) via `proxy connect <name>`.
            Err(err) => tracing::error!(
                slot = %slot.name,
                url = %slot.opencode_url,
                error = format!("{err:#}"),
                "slot failed to connect at startup — skipping it (retry with `proxy connect {}`)",
                slot.name,
            ),
        }
    }
    if registry.is_empty() && !cfg.slots.is_empty() {
        tracing::warn!(
            "no slots connected at startup — the bot is running but will route nobody \
             until a slot connects (`proxy connect <name>`)"
        );
    }
    Ok(registry)
}

/// Bring every configured slot up **concurrently, in the background** (#51),
/// inserting each into the live registry the moment it becomes ready. Returns
/// immediately, so the caller (`serve`) can start the dispatcher at once instead
/// of waiting out each slot's readiness budget.
///
/// Each slot gets its own task: a reachable opencode is validated (provider /
/// model) and inserted; an unreachable one retries for the full `attempts`
/// budget in its own task and logs on give-up (recover with `proxy connect
/// <name>`), never blocking its siblings or the bot. The registry is the same
/// runtime-mutable map `proxy connect` mutates, so a user who messages before
/// their slot is up simply hits the normal "no client yet" path until it lands.
pub fn spawn_slot_bringup(state: Arc<telegram::bot::AppState>, attempts: u32, interval: Duration) {
    let slots = state.cfg.slots.clone();
    if slots.is_empty() {
        return;
    }
    tracing::info!(slots = slots.len(), "connecting slots in the background");

    let http = match reqwest::Client::builder().build() {
        Ok(http) => http,
        Err(err) => {
            tracing::error!(error = %err, "could not build readiness http client — no slots will connect");
            return;
        }
    };

    for slot in slots {
        let state = Arc::clone(&state);
        let http = http.clone();
        tokio::spawn(async move {
            match bring_up_slot(&http, &slot, &state.cfg.model, attempts, interval).await {
                Ok(conn) => {
                    {
                        let mut guard = state
                            .registry
                            .write()
                            .unwrap_or_else(std::sync::PoisonError::into_inner);
                        guard.insert(slot.name.clone(), conn);
                    }
                    tracing::info!(slot = %slot.name, url = %slot.opencode_url, "slot connected");
                }
                Err(err) => tracing::error!(
                    slot = %slot.name,
                    url = %slot.opencode_url,
                    error = format!("{err:#}"),
                    "slot failed to connect at startup — skipping it (retry with `proxy connect {}`)",
                    slot.name,
                ),
            }
        });
    }
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
