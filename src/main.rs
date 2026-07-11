//! telegram-opencode-proxy — thin CLI shell around the library crate.
//!
//! All behaviour lives in `lib.rs` so the integration harness (issue #24) can
//! drive the real modules against in-process mocks. This binary only parses the
//! CLI and dispatches to [`telegram_opencode_proxy::serve`].

use anyhow::Result;
use clap::Parser;
use tracing_subscriber::EnvFilter;

use telegram_opencode_proxy::config::{Cli, Command, Config, PairAction};
use telegram_opencode_proxy::serve;

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

/// Initialize `tracing` with an env-controlled level (`RUST_LOG`, default `info`).
fn init_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();
}
