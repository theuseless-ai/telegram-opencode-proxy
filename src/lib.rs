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

use anyhow::{Context, Result};

use config::Config;
use opencode::client::{self, OpencodeClient};
use opencode::health;

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

    let clients = connect_slots(&cfg).await?;

    let db = persistence::Db::open(&cfg.db_path)
        .with_context(|| format!("opening SQLite store at {}", cfg.db_path.display()))?;
    tracing::info!(db = %cfg.db_path.display(), "opened SQLite store (WAL)");

    // Capture the admin socket path before `cfg` is moved into `AppState`, so we
    // can both serve on it and clean it up on shutdown.
    let admin_socket = cfg.admin_socket.clone();
    let bot = teloxide::Bot::new(cfg.bot_token.clone());
    let state = telegram::bot::AppState::new(cfg, clients, db);
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
/// one ready [`OpencodeClient`] per slot (keyed by slot name).
///
/// Factored out of [`serve`] so the harness can exercise the exact bring-up
/// sequence (readiness → provider catalogue → model validation) that gates a
/// live proxy, without also starting the forever-running dispatcher.
pub async fn connect_slots(cfg: &Config) -> Result<HashMap<String, OpencodeClient>> {
    let http = reqwest::Client::builder()
        .build()
        .context("building readiness http client")?;

    let mut clients: HashMap<String, OpencodeClient> = HashMap::with_capacity(cfg.slots.len());

    for slot in &cfg.slots {
        tracing::info!(slot = %slot.name, url = %slot.opencode_url, "connecting to opencode");
        health::wait_ready(
            &http,
            &slot.opencode_url,
            health::READY_ATTEMPTS,
            health::READY_INTERVAL,
        )
        .await
        .with_context(|| format!("opencode for slot '{}' not reachable", slot.name))?;

        let ocl = OpencodeClient::for_slot(slot)?;
        let providers = ocl
            .config_providers()
            .await
            .with_context(|| format!("fetching provider catalogue for slot '{}'", slot.name))?;
        client::validate_model(&providers, &cfg.model.provider_id, &cfg.model.model_id)
            .with_context(|| format!("validating model for slot '{}'", slot.name))?;

        tracing::info!(slot = %slot.name, "connected — provider/model validated");
        clients.insert(slot.name.clone(), ocl);
    }

    Ok(clients)
}
