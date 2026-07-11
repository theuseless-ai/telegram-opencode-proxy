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
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    tracing::info!("telegram-opencode-proxy starting (v0.0.1 scaffold)");
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
