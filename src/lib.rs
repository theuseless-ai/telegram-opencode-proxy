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
pub mod mcp;
pub mod opencode;
pub mod outbox;
pub mod pairing;
pub mod permission;
pub mod persistence;
pub mod session;
pub mod state;
pub mod telegram;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};

use config::{Config, Model, Slot};
use opencode::client::{self, OpencodeClient};
use opencode::health;
use state::SlotConn;

/// How long graceful shutdown waits for in-flight turns to finish before giving
/// up (#21). Generous — a turn can span a full model generation — but well under
/// systemd's default `TimeoutStopSec` (90s).
const SHUTDOWN_GRACE: Duration = Duration::from_secs(30);

/// How often the MCP file-store TTL sweep runs (#65). Inbound files stay
/// fetchable for `[mcp].ttl_secs` (default 300s); a fixed 60s sweep reclaims
/// expired temp files promptly without busy-looping — the sweep interval is the
/// only slack on top of a file's deadline.
const MCP_SWEEP_INTERVAL: Duration = Duration::from_secs(60);

/// Bring up the daemon: for each slot, connect to its (externally-managed)
/// opencode instance, wait until it's reachable, validate the configured
/// provider/model against its live catalogue, build a client — then run the
/// Telegram dispatcher until Ctrl-C. The proxy is **connect-only**: it does not
/// spawn opencode (start it via `./dev.sh` / systemd / compose).
///
/// `config_path` is the file `cfg` was loaded from; the daemon writes
/// `proxy connect`-added slots back to it (#45), so config is the single source
/// of truth for slots.
pub async fn serve(cfg: Config, config_path: PathBuf) -> Result<()> {
    // `[model]` may be omitted (#74) — the per-slot default is resolved at
    // connect and logged there; here just note the configured selector or "auto".
    let model = cfg
        .model
        .as_ref()
        .map(|m| format!("{}/{}", m.provider_id, m.model_id))
        .unwrap_or_else(|| "auto (opencode default)".to_string());
    tracing::info!(
        slots = cfg.slots.len(),
        model = %model,
        gated = cfg.permissions.ask.len(),
        "starting proxy (connect-only)"
    );

    // Open persistence for routing + whitelist + pending pairings/approvals (#3).
    let db = persistence::Db::open(&cfg.db_path)
        .with_context(|| format!("opening SQLite store at {}", cfg.db_path.display()))?;
    tracing::info!(db = %cfg.db_path.display(), "opened SQLite store (WAL)");

    // Capture the admin socket path before `cfg` is moved into `AppState`, so we
    // can both serve on it and clean it up on shutdown.
    let admin_socket = cfg.admin_socket.clone();
    let bot = teloxide::Bot::new(cfg.bot_token.expose());
    // Start with an EMPTY registry and bring the configured slots up
    // **concurrently, in the background** (#51). The dispatcher comes online
    // immediately instead of waiting out each slot's readiness budget, so one
    // unreachable opencode never delays replies for the healthy slots. The
    // whitelist is seeded from `cfg.slots` inside `AppState::new`, independent of
    // connection state, so bound users are authorized right away.
    let state = telegram::bot::AppState::new(cfg, config_path, HashMap::new(), db, bot.clone());
    spawn_slot_bringup(
        Arc::clone(&state),
        health::READY_ATTEMPTS,
        health::READY_INTERVAL,
    );
    // Outbound files (#12): one filesystem watcher per slot's `./outbox` → send
    // new files to the owning user. The handles must stay alive for the process
    // lifetime (dropping a watcher stops the watch), so hold them until shutdown.
    let _outbox_watchers = outbox::spawn_watchers(&state);

    // The admin server shares the same `Arc<AppState>` — read-only is enough for
    // #38 (runtime-mutable slots are #39).
    let admin_state: Arc<dyn admin::AdminState> = state.clone();

    // The admin control socket is a SECONDARY feature — run it in a background
    // task so a bind/permission failure only logs and is tolerated, never taking
    // down the bot. The dispatcher is the primary and blocks until Ctrl-C.
    let admin_socket_task = admin_socket.clone();
    let admin_task = tokio::spawn(async move {
        if let Err(err) = admin::serve_admin(admin_state, admin_socket_task).await {
            tracing::error!(
                error = format!("{err:#}"),
                "admin control socket unavailable — continuing without it"
            );
        }
    });

    // The MCP file-transfer server (#65) is, like the admin socket, a SECONDARY
    // feature: one stateless `/mcp` HTTP endpoint serving every slot, plus the
    // file-store's TTL sweep. Both are held for the process lifetime and torn
    // down on shutdown. A bind failure only logs and is tolerated — it must
    // never take down the dispatcher. Held in `Option`s so shutdown is a no-op
    // when the feature is disabled or the port could not be bound.
    let mcp_ct = tokio_util::sync::CancellationToken::new();
    let mut mcp_server: Option<tokio::task::JoinHandle<()>> = None;
    let mut mcp_sweep: Option<tokio::task::JoinHandle<()>> = None;
    if state.cfg.mcp.enabled {
        // Reclaim expired inbound files regardless of whether the listener binds
        // (the media path may still populate the store when MCP is enabled), so
        // the store can never grow unbounded. Held like `_outbox_watchers`.
        mcp_sweep = Some(mcp::store::FileStore::spawn_ttl_sweep(
            state.file_store.clone(),
            MCP_SWEEP_INTERVAL,
        ));

        let router = mcp::build_router(state.clone(), mcp_ct.clone());
        let bind = (state.cfg.mcp.bind, state.cfg.mcp.port);
        match tokio::net::TcpListener::bind(bind).await {
            Ok(listener) => {
                tracing::info!(
                    host = %state.cfg.mcp.bind,
                    port = state.cfg.mcp.port,
                    "MCP file-transfer server listening (single stateless /mcp for all slots)"
                );
                mcp_server = Some(tokio::spawn(async move {
                    if let Err(err) = axum::serve(listener, router).await {
                        tracing::error!(error = %err, "MCP file-transfer server terminated unexpectedly");
                    }
                }));
            }
            Err(err) => tracing::warn!(
                error = %err,
                host = %state.cfg.mcp.bind,
                port = state.cfg.mcp.port,
                "could not bind MCP file-transfer server — continuing without it"
            ),
        }
    }

    tracing::info!("starting Telegram long-poll dispatcher (Ctrl-C / SIGTERM to stop)");
    telegram::bot::run(bot, state.clone()).await;
    tracing::info!("dispatcher stopped — shutting down gracefully");

    // Graceful shutdown (#21): let in-flight per-user turns finish (bounded),
    // then flush SQLite. Connect-only, so there are no opencode children to reap.
    state.shutdown(SHUTDOWN_GRACE).await;

    // Stop the admin socket task and unlink the socket so a restart binds cleanly.
    admin_task.abort();
    match std::fs::remove_file(&admin_socket) {
        Ok(()) => tracing::debug!(socket = %admin_socket.display(), "removed admin socket"),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => {
            tracing::warn!(error = %err, socket = %admin_socket.display(), "failed to remove admin socket on shutdown");
        }
    }

    // Stop the MCP file-transfer server + TTL sweep (#65), alongside the admin
    // task above. Cancelling the token drains the stateless `/mcp` service; then
    // abort and await the tasks so they unwind before the process exits.
    mcp_ct.cancel();
    if let Some(server) = mcp_server {
        server.abort();
        let _ = server.await;
        tracing::debug!("MCP file-transfer server stopped");
    }
    if let Some(sweep) = mcp_sweep {
        sweep.abort();
        let _ = sweep.await;
        tracing::debug!("MCP file-store TTL sweep stopped");
    }

    tracing::info!("shutdown complete");
    Ok(())
}

/// Connect to and validate every configured slot's opencode instance
/// **synchronously**, returning a ready [`SlotConn`] per slot (keyed by slot
/// name) once all have resolved.
///
/// `config.toml` is the single source of truth for slots (#45), so the registry
/// is built from `cfg.slots` alone. [`serve`] does **not** use this — it brings
/// slots up in the background via [`spawn_slot_bringup`] so the dispatcher never
/// blocks on readiness (#51). This blocking, fully-resolved variant is what the
/// hermetic harness drives to exercise the exact bring-up sequence (readiness →
/// provider catalogue → model validation) without the forever-running dispatcher.
pub async fn connect_slots(cfg: &Config) -> Result<HashMap<String, SlotConn>> {
    let http = reqwest::Client::builder()
        .build()
        .context("building readiness http client")?;

    let mut registry: HashMap<String, SlotConn> = HashMap::with_capacity(cfg.slots.len());
    for slot in &cfg.slots {
        match bring_up_slot(
            &http,
            slot,
            cfg.model.as_ref(),
            health::READY_ATTEMPTS,
            health::READY_INTERVAL,
        )
        .await
        {
            Ok(conn) => {
                registry.insert(slot.name.clone(), conn);
            }
            // Best-effort: one unreachable / mis-provisioned slot must NOT crash
            // the daemon. Log it and keep serving the slots that did connect; the
            // skipped one comes up later (no restart) via `proxy connect <name>`.
            Err(err) => tracing::error!(
                slot = %slot.name,
                url = %slot.opencode_url,
                error = format!("{err:#}"),
                "slot failed to connect at startup — skipping it (retry with `proxy connect {}`)",
                slot.name,
            ),
        }
    }
    if registry.is_empty() && !cfg.slots.is_empty() {
        tracing::warn!(
            "no slots connected at startup — the bot is running but will route nobody \
             until a slot connects (`proxy connect <name>`)"
        );
    }
    Ok(registry)
}

/// Bring every configured slot up **concurrently, in the background** (#51),
/// inserting each into the live registry the moment it becomes ready. Returns
/// immediately, so the caller (`serve`) can start the dispatcher at once instead
/// of waiting out each slot's readiness budget.
///
/// Each slot gets its own task: a reachable opencode is validated (provider /
/// model) and inserted; an unreachable one retries for the full `attempts`
/// budget in its own task and logs on give-up (recover with `proxy connect
/// <name>`), never blocking its siblings or the bot. The registry is the same
/// runtime-mutable map `proxy connect` mutates, so a user who messages before
/// their slot is up simply hits the normal "no client yet" path until it lands.
pub fn spawn_slot_bringup(state: Arc<telegram::bot::AppState>, attempts: u32, interval: Duration) {
    let slots = state.cfg.slots.clone();
    if slots.is_empty() {
        return;
    }
    tracing::info!(slots = slots.len(), "connecting slots in the background");

    let http = match reqwest::Client::builder().build() {
        Ok(http) => http,
        Err(err) => {
            tracing::error!(error = %err, "could not build readiness http client — no slots will connect");
            return;
        }
    };

    for slot in slots {
        let state = Arc::clone(&state);
        let http = http.clone();
        tokio::spawn(async move {
            match bring_up_slot(&http, &slot, state.cfg.model.as_ref(), attempts, interval).await {
                Ok(conn) => {
                    {
                        let mut guard = state
                            .registry
                            .write()
                            .unwrap_or_else(std::sync::PoisonError::into_inner);
                        guard.insert(slot.name.clone(), conn);
                    }
                    tracing::info!(slot = %slot.name, url = %slot.opencode_url, "slot connected");
                }
                Err(err) => tracing::error!(
                    slot = %slot.name,
                    url = %slot.opencode_url,
                    error = format!("{err:#}"),
                    "slot failed to connect at startup — skipping it (retry with `proxy connect {}`)",
                    slot.name,
                ),
            }
        });
    }
}

/// The single-slot bring-up sequence shared by startup and the `connect` admin
/// command: wait until opencode is reachable, fetch its provider catalogue,
/// validate the configured `{provider, model}` against it, and build a client.
///
/// `attempts`/`interval` bound the readiness wait — startup uses the generous
/// 60 s budget ([`health::READY_ATTEMPTS`]); the interactive `connect` command
/// uses a short one so it fails fast on an unreachable slot.
pub(crate) async fn bring_up_slot(
    http: &reqwest::Client,
    slot: &Slot,
    model: Option<&Model>,
    attempts: u32,
    interval: Duration,
) -> Result<SlotConn> {
    tracing::info!(slot = %slot.name, url = %slot.opencode_url, "connecting to opencode");
    health::wait_ready(http, &slot.opencode_url, attempts, interval)
        .await
        .with_context(|| format!("opencode for slot '{}' not reachable", slot.name))?;

    let client = OpencodeClient::for_slot(slot)?;
    let providers = client
        .config_providers()
        .await
        .with_context(|| format!("fetching provider catalogue for slot '{}'", slot.name))?;

    // Resolve the effective model selector (#74): the configured `[model]` if
    // present (validated), else opencode's own sole default.
    let model = resolve_model(&providers, model, &slot.name)
        .with_context(|| format!("resolving model for slot '{}'", slot.name))?;

    // Resolve the context-window size for the usage footer (#72): the proxy
    // `[model].context_window` override wins, else opencode's own catalogue
    // (`/config/providers`, then the fuller `/provider`). `None` → the footer
    // shows a raw token count instead of a %.
    let context_limit = resolve_context_limit(&client, &providers, &model).await;

    tracing::info!(
        slot = %slot.name,
        provider_id = %model.provider_id,
        model_id = %model.model_id,
        context_limit,
        "connected — provider/model resolved"
    );
    Ok(SlotConn {
        slot: slot.clone(),
        client,
        model,
        context_limit,
    })
}

/// Resolve the effective model selector for a slot (#74): the configured
/// `[model]` if present (validated against opencode's catalogue), otherwise
/// opencode's **sole** default model. Errors — with an actionable message — when
/// no `[model]` is set and opencode has no single unambiguous default.
fn resolve_model(
    providers: &crate::opencode::types::ProvidersResponse,
    configured: Option<&Model>,
    slot_name: &str,
) -> Result<Model> {
    if let Some(model) = configured {
        client::validate_model(providers, &model.provider_id, &model.model_id)?;
        return Ok(model.clone());
    }
    let (provider_id, model_id) = providers.sole_default_model().ok_or_else(|| {
        let detail = if providers.default.is_empty() {
            "opencode reports no default model".to_string()
        } else {
            let mut providers: Vec<&str> = providers.default.keys().map(String::as_str).collect();
            providers.sort_unstable();
            format!(
                "opencode has multiple provider defaults [{}]",
                providers.join(", ")
            )
        };
        anyhow!(
            "no [model] configured for slot '{slot_name}' and {detail} — \
             set [model] provider_id/model_id in config.toml"
        )
    })?;
    Ok(Model {
        provider_id: provider_id.to_string(),
        model_id: model_id.to_string(),
        context_window: None,
    })
}

/// Resolve the active model's context-window size (#72), preferring the proxy
/// config override, then opencode's `/config/providers`, then `/provider`.
async fn resolve_context_limit(
    client: &OpencodeClient,
    providers: &crate::opencode::types::ProvidersResponse,
    model: &Model,
) -> Option<u64> {
    if let Some(window) = model.context_window {
        return Some(window);
    }
    if let Some(limit) = providers.context_limit(&model.provider_id, &model.model_id) {
        return Some(limit);
    }
    match client.provider_catalogue().await {
        Ok(catalogue) => catalogue.context_limit(&model.provider_id, &model.model_id),
        Err(err) => {
            tracing::debug!(error = %err, "GET /provider for context limit failed");
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::resolve_model;
    use crate::config::Model;
    use crate::opencode::types::ProvidersResponse;

    fn catalogue(json: serde_json::Value) -> ProvidersResponse {
        serde_json::from_value(json).expect("catalogue parses")
    }

    #[test]
    fn resolve_model_uses_configured_model_and_keeps_override() {
        let cat = catalogue(serde_json::json!({
            "providers": [{ "id": "llm-lan", "models": { "Qwen": {} } }],
            "default": {}
        }));
        let cfg = Model {
            provider_id: "llm-lan".into(),
            model_id: "Qwen".into(),
            context_window: Some(1000),
        };
        let resolved = resolve_model(&cat, Some(&cfg), "you").expect("resolves configured");
        assert_eq!(resolved.provider_id, "llm-lan");
        assert_eq!(resolved.model_id, "Qwen");
        assert_eq!(resolved.context_window, Some(1000));
    }

    #[test]
    fn resolve_model_falls_back_to_opencode_sole_default() {
        let cat = catalogue(serde_json::json!({
            "providers": [{ "id": "llm-lan", "models": { "Qwen": {} } }],
            "default": { "llm-lan": "Qwen" }
        }));
        let resolved = resolve_model(&cat, None, "you").expect("resolves opencode default");
        assert_eq!(resolved.provider_id, "llm-lan");
        assert_eq!(resolved.model_id, "Qwen");
        assert_eq!(resolved.context_window, None);
    }

    #[test]
    fn resolve_model_errors_without_config_or_default() {
        let cat = catalogue(serde_json::json!({ "providers": [], "default": {} }));
        let err = resolve_model(&cat, None, "you").unwrap_err().to_string();
        assert!(err.contains("no [model]"), "{err}");
        assert!(err.contains("no default model"), "{err}");
    }

    #[test]
    fn resolve_model_errors_on_ambiguous_defaults() {
        let cat = catalogue(serde_json::json!({
            "providers": [], "default": { "a": "m1", "b": "m2" }
        }));
        let err = resolve_model(&cat, None, "you").unwrap_err().to_string();
        assert!(err.contains("multiple provider defaults"), "{err}");
    }

    #[test]
    fn resolve_model_rejects_configured_model_absent_from_opencode() {
        let cat = catalogue(serde_json::json!({
            "providers": [{ "id": "llm-lan", "models": { "Qwen": {} } }],
            "default": {}
        }));
        let cfg = Model {
            provider_id: "llm-lan".into(),
            model_id: "ghost".into(),
            context_window: None,
        };
        assert!(resolve_model(&cat, Some(&cfg), "you").is_err());
    }
}
