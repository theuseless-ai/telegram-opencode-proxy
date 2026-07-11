//! Spawn / keep-alive / restart one `opencode serve` process per slot.
//!
//! Each slot runs `opencode serve --port <P> --hostname 127.0.0.1` in the
//! slot's workdir, where `<P>` is parsed from `slot.opencode_url`. A slot is
//! not "live" until [`wait_ready`] sees `GET /config` return 200.
//!
//! Issue #5 wires the spawn + readiness poll + a restart-on-exit skeleton
//! ([`supervise`]). Full crash-loop backoff and health tracking are #N3.
//! See `docs/design/architecture.md` §4.

// Forward-declared: the spawn/supervise entry points are invoked by the serve
// startup wiring (#6). Port parsing + readiness are exercised by unit tests.
#![allow(dead_code)]

use std::process::{ExitStatus, Stdio};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use tokio::process::{Child, Command};

use crate::config::Slot;

/// Default readiness budget: 120 attempts × 500 ms ≈ 60 s.
const READY_ATTEMPTS: u32 = 120;
const READY_INTERVAL: Duration = Duration::from_millis(500);

/// Parse the TCP port from an opencode base URL
/// (`http://127.0.0.1:4096` → `4096`).
pub fn port_from_url(url: &str) -> Result<u16> {
    let after_scheme = url.split_once("://").map_or(url, |(_, rest)| rest);
    let authority = after_scheme
        .split(['/', '?', '#'])
        .next()
        .unwrap_or(after_scheme);
    // Drop any `user:pass@` userinfo before the host:port.
    let host_port = authority.rsplit_once('@').map_or(authority, |(_, hp)| hp);
    let port_str = host_port
        .rsplit_once(':')
        .map(|(_, p)| p)
        .with_context(|| format!("opencode_url '{url}' has no explicit port"))?;
    port_str
        .parse::<u16>()
        .with_context(|| format!("opencode_url '{url}' has an invalid port '{port_str}'"))
}

/// A running (or exited) `opencode serve` child bound to one slot.
#[derive(Debug)]
pub struct SlotProcess {
    pub name: String,
    pub port: u16,
    child: Child,
}

impl SlotProcess {
    /// Spawn `opencode serve` for `slot` in its workdir. `kill_on_drop` ensures
    /// the child is reaped if the supervisor is dropped.
    pub fn spawn(slot: &Slot) -> Result<Self> {
        let port = port_from_url(&slot.opencode_url)?;
        let child = Command::new("opencode")
            .arg("serve")
            .arg("--port")
            .arg(port.to_string())
            .arg("--hostname")
            .arg("127.0.0.1")
            .current_dir(&slot.workdir)
            .stdin(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .with_context(|| format!("spawning `opencode serve` for slot '{}'", slot.name))?;
        Ok(Self {
            name: slot.name.clone(),
            port,
            child,
        })
    }

    /// Wait for the child to exit (used by the restart loop).
    pub async fn wait(&mut self) -> Result<ExitStatus> {
        self.child
            .wait()
            .await
            .with_context(|| format!("waiting on opencode child for slot '{}'", self.name))
    }

    /// Best-effort terminate and reap the child.
    pub async fn shutdown(&mut self) -> Result<()> {
        let _ = self.child.start_kill();
        let _ = self.child.wait().await;
        Ok(())
    }
}

/// Poll `GET {base_url}/config` until it returns a 2xx, bounded by
/// `max_attempts`. A slot must not be marked live until this returns `Ok`.
pub async fn wait_ready(
    http: &reqwest::Client,
    base_url: &str,
    max_attempts: u32,
    interval: Duration,
) -> Result<()> {
    let url = format!("{}/config", base_url.trim_end_matches('/'));
    for attempt in 1..=max_attempts {
        match http.get(&url).send().await {
            Ok(resp) if resp.status().is_success() => return Ok(()),
            Ok(resp) => {
                tracing::debug!(status = %resp.status(), attempt, "opencode not ready yet");
            }
            Err(err) => tracing::debug!(error = %err, attempt, "opencode unreachable yet"),
        }
        tokio::time::sleep(interval).await;
    }
    bail!("opencode at {base_url} not ready after {max_attempts} attempts");
}

/// Keep one slot's opencode alive: spawn, wait until ready, then respawn on
/// exit. This is the restart-on-exit **skeleton** — crash-loop backoff and
/// health tracking land in #N3. Runs until the future is dropped.
pub async fn supervise(slot: Slot, http: reqwest::Client) -> Result<()> {
    loop {
        let mut proc = SlotProcess::spawn(&slot)?;
        wait_ready(&http, &slot.opencode_url, READY_ATTEMPTS, READY_INTERVAL).await?;
        tracing::info!(slot = %slot.name, port = proc.port, "opencode instance live");
        let status = proc.wait().await?;
        // TODO(#N3): crash-loop backoff + health tracking before respawning.
        tracing::warn!(slot = %slot.name, ?status, "opencode exited — restarting");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_port_from_urls() {
        assert_eq!(port_from_url("http://127.0.0.1:4096").unwrap(), 4096);
        assert_eq!(port_from_url("http://127.0.0.1:4097/").unwrap(), 4097);
        assert_eq!(port_from_url("https://host:8080/path?x=1").unwrap(), 8080);
        assert_eq!(
            port_from_url("http://user:pw@127.0.0.1:4096").unwrap(),
            4096
        );
    }

    #[test]
    fn rejects_urls_without_a_port() {
        assert!(port_from_url("http://127.0.0.1").is_err());
        assert!(port_from_url("http://127.0.0.1:notaport").is_err());
        assert!(port_from_url("http://127.0.0.1:99999").is_err());
    }

    #[tokio::test]
    async fn wait_ready_fails_fast_when_unreachable() {
        let http = reqwest::Client::builder()
            .connect_timeout(Duration::from_millis(50))
            .build()
            .unwrap();
        // Port 1 is not listening → connection refused → bounded retries exhaust.
        let res = wait_ready(&http, "http://127.0.0.1:1", 1, Duration::from_millis(1)).await;
        assert!(res.is_err());
    }
}
