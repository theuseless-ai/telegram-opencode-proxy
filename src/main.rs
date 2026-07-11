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

use anyhow::Result;
use clap::Parser;
use tracing_subscriber::EnvFilter;

use config::{Cli, Command, PairAction};

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    match Cli::parse().command {
        Command::Serve {
            config: config_path,
        } => {
            let cfg = config::Config::load(&config_path)?;
            tracing::info!(
                slots = cfg.slots.len(),
                model = %cfg.model.model_id,
                admin_socket = %cfg.admin_socket.display(),
                gated = cfg.permissions.ask.len(),
                "config loaded — serve wiring lands in #5/#6"
            );
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

/// Initialize `tracing` with an env-controlled level (`RUST_LOG`, default `info`).
fn init_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();
}
