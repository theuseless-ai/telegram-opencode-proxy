//! telegram-opencode-proxy — thin CLI shell around the library crate.
//!
//! All behaviour lives in `lib.rs` so the integration harness (issue #24) can
//! drive the real modules against in-process mocks. This binary only parses the
//! CLI and dispatches to [`telegram_opencode_proxy::serve`].

use std::path::PathBuf;

use anyhow::{Result, bail};
use clap::Parser;
use tracing_subscriber::EnvFilter;

use telegram_opencode_proxy::admin::{
    self, AdminRequest, AdminResponse, ConnectOutcome, SlotSource,
};
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
        Command::Slots { config, socket } => {
            slots(config, socket).await?;
        }
        Command::Pair {
            action,
            config,
            socket,
        } => {
            pair(action, config, socket).await?;
        }
    }
    Ok(())
}

/// Resolve the admin socket path from an explicit `--socket` or the config's
/// `admin_socket`. Shared by every client subcommand.
fn socket_path(config: PathBuf, socket: Option<PathBuf>) -> Result<PathBuf> {
    match socket {
        Some(path) => Ok(path),
        None => Ok(Config::load(&config)?.admin_socket),
    }
}

/// `proxy status`: dial the running daemon's admin socket and print a slot
/// table. The socket path comes from `--socket` if given, else from the config's
/// `admin_socket`.
async fn status(config: PathBuf, socket: Option<PathBuf>) -> Result<()> {
    let socket_path = socket_path(config, socket)?;

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
    let socket_path = socket_path(config, socket)?;

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

/// `proxy slots`: dial the running daemon and print the per-slot inventory —
/// name, opencode URL, workdir, the Telegram ids bound to it, reachability, and
/// whether it is config- or db-sourced. This is how an admin picks a `--slot`.
async fn slots(config: PathBuf, socket: Option<PathBuf>) -> Result<()> {
    let socket_path = socket_path(config, socket)?;

    match admin::send_request(&socket_path, &AdminRequest::Slots).await? {
        AdminResponse::Slots { slots } => {
            println!(
                "{:<14} {:<8} {:<6} {:<28} {:<20} TELEGRAM IDS",
                "SLOT", "SOURCE", "STATE", "OPENCODE URL", "WORKDIR"
            );
            for slot in slots {
                let source = match slot.source {
                    SlotSource::Config => "config",
                    SlotSource::Db => "db",
                };
                let state = if slot.connected { "up" } else { "down" };
                let ids = if slot.telegram_ids.is_empty() {
                    "-".to_string()
                } else {
                    slot.telegram_ids
                        .iter()
                        .map(|id| id.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                };
                println!(
                    "{:<14} {:<8} {:<6} {:<28} {:<20} {ids}",
                    slot.name, source, state, slot.opencode_url, slot.workdir
                );
            }
            Ok(())
        }
        AdminResponse::Error { message } => bail!("daemon returned an error: {message}"),
        other => bail!("unexpected response from daemon: {other:?}"),
    }
}

/// `proxy pair list|approve|deny`: the admin enrolment client (#4b). Dials the
/// running daemon and prints the outcome; a daemon-side failure exits non-zero.
async fn pair(action: PairAction, config: PathBuf, socket: Option<PathBuf>) -> Result<()> {
    let socket_path = socket_path(config, socket)?;

    match action {
        PairAction::List => match admin::send_request(&socket_path, &AdminRequest::PairList).await?
        {
            AdminResponse::PairList { pending } => {
                if pending.is_empty() {
                    println!("No pending pairing requests.");
                    return Ok(());
                }
                println!("{:<8} {:<20} {:<12} AGE", "CODE", "USERNAME", "CHAT ID");
                for entry in pending {
                    let username = entry
                        .username
                        .map(|u| format!("@{u}"))
                        .unwrap_or_else(|| "-".to_string());
                    println!(
                        "{:<8} {:<20} {:<12} {}",
                        entry.code,
                        username,
                        entry.chat_id,
                        format_age(entry.age_secs)
                    );
                }
                Ok(())
            }
            AdminResponse::Error { message } => bail!("daemon returned an error: {message}"),
            other => bail!("unexpected response from daemon: {other:?}"),
        },
        PairAction::Approve { code, slot } => {
            let req = AdminRequest::PairApprove { code, slot };
            match admin::send_request(&socket_path, &req).await? {
                AdminResponse::PairApprove {
                    code,
                    chat_id,
                    slot,
                    username,
                } => {
                    let who = username.map(|u| format!(" (@{u})")).unwrap_or_default();
                    println!(
                        "approved {code}: chat {chat_id}{who} → slot '{slot}' (user notified)"
                    );
                    Ok(())
                }
                AdminResponse::Error { message } => bail!("daemon returned an error: {message}"),
                other => bail!("unexpected response from daemon: {other:?}"),
            }
        }
        PairAction::Deny { code } => {
            let req = AdminRequest::PairDeny { code };
            match admin::send_request(&socket_path, &req).await? {
                AdminResponse::PairDeny { code, removed } => {
                    if removed {
                        println!("denied {code}: pending request dropped");
                    } else {
                        println!("no pending request with code {code}");
                    }
                    Ok(())
                }
                AdminResponse::Error { message } => bail!("daemon returned an error: {message}"),
                other => bail!("unexpected response from daemon: {other:?}"),
            }
        }
    }
}

/// Render an age in seconds as a compact human string (`45s`, `12m`, `3h`).
fn format_age(secs: i64) -> String {
    let secs = secs.max(0);
    if secs < 60 {
        format!("{secs}s")
    } else if secs < 3600 {
        format!("{}m", secs / 60)
    } else {
        format!("{}h", secs / 3600)
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
