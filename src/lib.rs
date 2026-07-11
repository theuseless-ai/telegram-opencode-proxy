//! telegram-opencode-proxy — bridge a Telegram bot to `opencode serve`.
//!
//! The crate is built as **both** a library and a binary. `src/main.rs` is a
//! thin CLI shell; all logic lives here so the integration harness (issue #24,
//! `tests/`) can drive the real modules — `AppState`, the dispatcher handlers,
//! the opencode client, and the `serve` bring-up validation — against the
//! in-process mocks. Module layout follows `docs/design/architecture.md` §4.

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

    let bot = teloxide::Bot::new(cfg.bot_token.clone());
    let state = telegram::bot::AppState::new(cfg, clients, db);

    tracing::info!("starting Telegram long-poll dispatcher (Ctrl-C to stop)");
    telegram::bot::run(bot, state).await;

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
