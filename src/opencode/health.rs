//! opencode connection health.
//!
//! The proxy is **connect-only**: it does NOT spawn or supervise `opencode
//! serve` — those instances are managed externally (systemd / compose /
//! `./dev.sh`). This module just checks that a configured instance is reachable
//! before the proxy routes to it. See `docs/design/architecture.md` §4.
//!
//! Reconnect-on-drop during a live session is #N3.

use std::time::Duration;

use anyhow::{Result, bail};

/// Default readiness budget: 120 attempts × 500 ms ≈ 60 s.
pub const READY_ATTEMPTS: u32 = 120;
pub const READY_INTERVAL: Duration = Duration::from_millis(500);

/// Poll `GET {base_url}/config` until it returns a 2xx, bounded by
/// `max_attempts`. A slot must not be routed to until this returns `Ok`.
///
/// opencode must already be running at `base_url` — the proxy connects, it does
/// not launch it.
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
    bail!(
        "opencode at {base_url} not reachable after {max_attempts} attempts — is it running? \
         The proxy is connect-only; start opencode separately (e.g. ./dev.sh or systemd)."
    );
}

#[cfg(test)]
mod tests {
    use super::*;

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
