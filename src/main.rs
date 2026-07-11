//! telegram-opencode-proxy — bridge a Telegram bot to `opencode serve`.
//!
//! Module layout follows `docs/design/architecture.md` §4. This is the v0.0.1
//! scaffold (#1): modules are stubs, wired together in later issues.

mod auth;
mod config;
mod opencode;
mod outbox;
mod pairing;
mod permission;
mod persistence;
mod session;
mod state;
mod telegram;

use std::collections::HashMap;

use anyhow::{Context, Result};
use clap::Parser;
use tracing_subscriber::EnvFilter;

use config::{Cli, Command, Config, PairAction};
use opencode::client::{self, OpencodeClient};
use opencode::supervisor::{self, SlotProcess};

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    match Cli::parse().command {
        Command::Serve {
            config: config_path,
        } => {
            let cfg = Config::load(&config_path)?;
            serve(cfg).await?;
        }
        Command::Pair { action } => match action {
            PairAction::List => tracing::info!("pair list — not implemented (#4b)"),
            PairAction::Approve { code, slot } => {
                tracing::info!(%code, %slot, "pair approve — not implemented (#4b)");
            }
            PairAction::Deny { code } => {
                tracing::info!(%code, "pair deny — not implemented (#4b)");
            }
        },
    }
    Ok(())
}

/// Bring up the daemon: for each slot spawn `opencode serve`, wait for it to be
/// ready, validate the configured provider/model against its live catalogue,
/// build a client — then run the Telegram dispatcher until Ctrl-C.
async fn serve(cfg: Config) -> Result<()> {
    tracing::info!(
        slots = cfg.slots.len(),
        provider = %cfg.model.provider_id,
        model = %cfg.model.model_id,
        gated = cfg.permissions.ask.len(),
        "starting proxy"
    );

    let http = reqwest::Client::builder()
        .build()
        .context("building readiness http client")?;

    // Kept alive for the daemon's lifetime; `kill_on_drop` reaps them on exit.
    let mut procs: Vec<SlotProcess> = Vec::with_capacity(cfg.slots.len());
    let mut clients: HashMap<String, OpencodeClient> = HashMap::with_capacity(cfg.slots.len());

    for slot in &cfg.slots {
        let proc = SlotProcess::spawn(slot)?;
        tracing::info!(slot = %proc.name, port = proc.port, "spawned opencode — waiting for readiness");
        supervisor::wait_ready(
            &http,
            &slot.opencode_url,
            supervisor::READY_ATTEMPTS,
            supervisor::READY_INTERVAL,
        )
        .await?;

        let ocl = OpencodeClient::for_slot(slot)?;
        let providers = ocl
            .config_providers()
            .await
            .with_context(|| format!("fetching provider catalogue for slot '{}'", slot.name))?;
        client::validate_model(&providers, &cfg.model.provider_id, &cfg.model.model_id)
            .with_context(|| format!("validating model for slot '{}'", slot.name))?;

        tracing::info!(slot = %slot.name, "opencode ready — provider/model validated");
        clients.insert(slot.name.clone(), ocl);
        procs.push(proc);
    }

    let bot = teloxide::Bot::new(cfg.bot_token.clone());
    let state = telegram::bot::AppState::new(cfg, clients);

    tracing::info!("starting Telegram long-poll dispatcher (Ctrl-C to stop)");
    telegram::bot::run(bot, state).await;

    // TODO(#N2): graceful shutdown — abort in-flight turns, then reap procs.
    // For now the dispatcher's Ctrl-C handler returns here and `procs`
    // (`kill_on_drop`) terminate the opencode children as they drop.
    drop(procs);
    Ok(())
}

/// Initialize `tracing` with an env-controlled level (`RUST_LOG`, default `info`).
fn init_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();
}
