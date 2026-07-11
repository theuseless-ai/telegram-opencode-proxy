//! telegram-opencode-proxy — thin CLI shell around the library crate.
//!
//! All behaviour lives in `lib.rs` so the integration harness (issue #24) can
//! drive the real modules against in-process mocks. This binary only parses the
//! CLI and dispatches to [`telegram_opencode_proxy::serve`].

use std::path::PathBuf;

use anyhow::{Result, bail};
use clap::Parser;
use tracing_subscriber::EnvFilter;

use telegram_opencode_proxy::admin::{self, AdminRequest, AdminResponse, ConnectOutcome};
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
        Command::Status { config, socket } => {
            status(config, socket).await?;
        }
        Command::Connect {
            name,
            url,
            workdir,
            telegram_id,
            config,
            socket,
        } => {
            connect(name, url, workdir, telegram_id, config, socket).await?;
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

/// `proxy status`: dial the running daemon's admin socket and print a slot
/// table. The socket path comes from `--socket` if given, else from the config's
/// `admin_socket`.
async fn status(config: PathBuf, socket: Option<PathBuf>) -> Result<()> {
    let socket_path = match socket {
        Some(path) => path,
        None => Config::load(&config)?.admin_socket,
    };

    match admin::send_request(&socket_path, &AdminRequest::Status).await? {
        AdminResponse::Status { slots } => {
            println!("{:<16} {:<32} STATUS", "SLOT", "OPENCODE URL");
            for slot in slots {
                let state = if slot.connected { "connected" } else { "down" };
                println!("{:<16} {:<32} {state}", slot.name, slot.opencode_url);
            }
            Ok(())
        }
        AdminResponse::Error { message } => bail!("daemon returned an error: {message}"),
        other => bail!("unexpected response from daemon: {other:?}"),
    }
}

/// `proxy connect <name>`: dial the running daemon and idempotently ensure the
/// slot is connected. Prints the outcome; a daemon-side failure exits non-zero.
async fn connect(
    name: String,
    url: Option<String>,
    workdir: Option<String>,
    telegram_id: Option<i64>,
    config: PathBuf,
    socket: Option<PathBuf>,
) -> Result<()> {
    let socket_path = match socket {
        Some(path) => path,
        None => Config::load(&config)?.admin_socket,
    };

    let req = AdminRequest::Connect {
        name,
        url,
        workdir,
        telegram_id,
    };
    match admin::send_request(&socket_path, &req).await? {
        AdminResponse::Connect { name, outcome } => {
            let label = match outcome {
                ConnectOutcome::Connected => "connected",
                ConnectOutcome::Reconnected => "reconnected",
                ConnectOutcome::Added => "added",
            };
            println!("{name}: {label}");
            Ok(())
        }
        AdminResponse::Error { message } => bail!("daemon returned an error: {message}"),
        other => bail!("unexpected response from daemon: {other:?}"),
    }
}

/// Initialize `tracing` with an env-controlled level (`RUST_LOG`, default `info`).
fn init_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();
}
