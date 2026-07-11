//! teloxide long-poll dispatcher: text messages, `/new` `/whoami` `/get` `/stop`,
//! `callback_query` (approval buttons), inbound file download. Whitelist gate.
//! See `docs/design/architecture.md` §4/§13. Issue #6.
//!
//! #6 wires the "wire green" subset: a text message from a whitelisted user is
//! routed to that user's opencode slot (blocking `POST /session/:id/message`)
//! and the reply is chunked back with [`render::split_message`]. `/whoami`
//! reports the numeric chat id (a bootstrap aid). Streaming, files, `/new`,
//! `/get`, `/stop`, and per-user mpsc serialization land in later issues.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, PoisonError, RwLock};
use std::time::Duration;

use anyhow::{Context, anyhow};
use teloxide::prelude::*;
use teloxide::utils::command::BotCommands;

use crate::admin::{BoxFuture, ConnectOutcome, ConnectParams, SlotInfo};
use crate::auth;
use crate::config::{Config, Slot};
use crate::opencode::client::OpencodeClient;
use crate::opencode::types::PromptModel;
use crate::persistence::Db;
use crate::session;
use crate::state::SlotConn;
use crate::telegram::render::{self, TELEGRAM_LIMIT};

/// Readiness budget for the interactive `proxy connect` bring-up — short so the
/// command fails fast on an unreachable slot (unlike the 60 s startup budget).
const CONNECT_READY_ATTEMPTS: u32 = 5;
const CONNECT_READY_INTERVAL: Duration = Duration::from_millis(200);

/// Shared dispatcher state: config, the runtime-mutable slot registry, and the
/// SQLite-backed `chat_id → session_id` routing store.
///
/// The [`registry`](Self::registry) is a `RwLock<HashMap<name, SlotConn>>` the
/// daemon mutates at runtime (`proxy connect`, #39). It is a `std::sync::RwLock`
/// — matching the [`Db`] concurrency discipline: an accessor takes a short
/// guard, clones what it needs out, and **drops the guard before any `.await`**,
/// so a lock is never held across a suspension point. [`OpencodeClient`] is
/// `Arc`-backed and cheap to clone, so the turn path clones the client out under
/// a read guard and releases it before the opencode round-trip.
///
/// Routing lives in the [`Db`] `routing` table (#3), so sessions survive a
/// restart: `run_turn` reads the stored id, lets `session::get_or_create`
/// resolve it (recreating a stale/404 id), then writes the resolved id back.
/// The per-user `mpsc` turn-serialization queue is #9.
pub struct AppState {
    pub cfg: Config,
    pub registry: RwLock<HashMap<String, SlotConn>>,
    pub db: Db,
}

impl AppState {
    /// Build state from config, the seeded per-slot registry (config ∪ DB), and
    /// an open SQLite handle.
    pub fn new(cfg: Config, registry: HashMap<String, SlotConn>, db: Db) -> Arc<Self> {
        Arc::new(Self {
            cfg,
            registry: RwLock::new(registry),
            db,
        })
    }

    /// A cloned snapshot of every live slot definition. Takes a short read guard
    /// and releases it before returning — never held across an await.
    fn slot_snapshot(&self) -> Vec<Slot> {
        let guard = self.registry.read().unwrap_or_else(PoisonError::into_inner);
        guard.values().map(|c| c.slot.clone()).collect()
    }

    /// Clone the ready client for `name` out of the registry (guard dropped
    /// before the caller awaits), or `None` if the slot is unknown.
    fn client_for(&self, name: &str) -> Option<OpencodeClient> {
        let guard = self.registry.read().unwrap_or_else(PoisonError::into_inner);
        guard.get(name).map(|c| c.client.clone())
    }

    /// The three-way idempotent `connect` behaviour (#39). All opencode
    /// round-trips happen outside the registry lock; the lock is only taken to
    /// snapshot the current slot and to swap the rebuilt client in.
    async fn ensure_connected_impl(&self, params: ConnectParams) -> anyhow::Result<ConnectOutcome> {
        let http = reqwest::Client::builder()
            .connect_timeout(Duration::from_millis(500))
            .timeout(Duration::from_secs(5))
            .build()
            .context("building connect probe http client")?;

        // Snapshot any existing slot, then drop the guard before awaiting.
        let existing = {
            let guard = self.registry.read().unwrap_or_else(PoisonError::into_inner);
            guard.get(&params.name).map(|c| c.slot.clone())
        };

        if let Some(slot) = existing {
            // Already up? One short readiness attempt against its current URL.
            if crate::opencode::health::wait_ready(
                &http,
                &slot.opencode_url,
                1,
                Duration::from_millis(1),
            )
            .await
            .is_ok()
            {
                return Ok(ConnectOutcome::Connected);
            }
            // Down → rebuild the client (re-validating provider/model) and swap
            // it into the live registry. Identity is unchanged, so no persist.
            let conn = crate::bring_up_slot(
                &http,
                &slot,
                &self.cfg.model,
                CONNECT_READY_ATTEMPTS,
                CONNECT_READY_INTERVAL,
            )
            .await
            .with_context(|| format!("reconnecting slot '{}'", slot.name))?;
            {
                let mut guard = self
                    .registry
                    .write()
                    .unwrap_or_else(PoisonError::into_inner);
                guard.insert(slot.name.clone(), conn);
            }
            return Ok(ConnectOutcome::Reconnected);
        }

        // Missing → add. A URL is required; workdir defaults to the cwd.
        let url = params.url.ok_or_else(|| {
            anyhow!(
                "slot '{}' does not exist — pass --url (and optionally --workdir/--telegram-id) to add it",
                params.name
            )
        })?;
        let slot = Slot {
            name: params.name.clone(),
            opencode_url: url,
            workdir: params
                .workdir
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from(".")),
            telegram_id: params.telegram_id,
        };
        let conn = crate::bring_up_slot(
            &http,
            &slot,
            &self.cfg.model,
            CONNECT_READY_ATTEMPTS,
            CONNECT_READY_INTERVAL,
        )
        .await
        .with_context(|| format!("adding slot '{}'", slot.name))?;
        // Persist so it survives a restart, then swap into the live registry.
        self.db
            .upsert_slot(&slot)
            .with_context(|| format!("persisting slot '{}'", slot.name))?;
        {
            let mut guard = self
                .registry
                .write()
                .unwrap_or_else(PoisonError::into_inner);
            guard.insert(slot.name.clone(), conn);
        }
        Ok(ConnectOutcome::Added)
    }
}

/// The admin control socket (#38/#39) reports the live slots and drives runtime
/// slot mutations. Reads/writes go through the runtime registry, so slots added
/// via `proxy connect` show up here too.
impl crate::admin::AdminState for AppState {
    fn slots(&self) -> Vec<SlotInfo> {
        let mut out: Vec<SlotInfo> = {
            let guard = self.registry.read().unwrap_or_else(PoisonError::into_inner);
            guard
                .values()
                .map(|c| SlotInfo {
                    name: c.slot.name.clone(),
                    opencode_url: c.slot.opencode_url.clone(),
                })
                .collect()
        };
        // Stable order: a HashMap iterates arbitrarily.
        out.sort_by(|a, b| a.name.cmp(&b.name));
        out
    }

    fn ensure_connected<'a>(
        &'a self,
        params: ConnectParams,
    ) -> BoxFuture<'a, anyhow::Result<ConnectOutcome>> {
        Box::pin(async move { self.ensure_connected_impl(params).await })
    }
}

/// Bot commands. Kept minimal for #6 — `/whoami` aids bootstrap (the operator
/// learns their chat id to whitelist). `/new` `/get` `/stop` land later.
#[derive(BotCommands, Clone)]
#[command(rename_rule = "lowercase")]
enum Command {
    /// Show your numeric chat id (use it to whitelist yourself in config.toml).
    Whoami,
}

/// Friendly message shown to the user when a turn fails; details go to the log.
const ERROR_REPLY: &str = "⚠️ Sorry — I couldn't reach opencode to answer that. Please try again.";

/// Run the long-poll dispatcher until Ctrl-C. `bot` and `state` are injected as
/// handler dependencies.
pub async fn run(bot: Bot, state: Arc<AppState>) {
    let handler = Update::filter_message()
        .branch(teloxide::filter_command::<Command, _>().endpoint(handle_command))
        .branch(dptree::endpoint(handle_text));

    Dispatcher::builder(bot, handler)
        .dependencies(dptree::deps![state])
        .enable_ctrlc_handler()
        .build()
        .dispatch()
        .await;
}

/// Handle `/whoami` (and any future commands). Deliberately NOT gated by auth —
/// an unwhitelisted user needs their chat id to get whitelisted.
async fn handle_command(bot: Bot, msg: Message, cmd: Command) -> ResponseResult<()> {
    match cmd {
        Command::Whoami => {
            let text = format!("Your chat id is `{}`.", msg.chat.id.0);
            bot.send_message(msg.chat.id, text).await?;
        }
    }
    Ok(())
}

/// Handle a plain text message: whitelist gate → route to the slot's opencode
/// → blocking prompt → chunked reply. Never panics; failures become a friendly
/// reply plus a `tracing::error!`.
///
/// `pub` so the integration harness (issue #24) can drive the real turn path
/// against the in-process mocks without spinning up the long-poll dispatcher.
pub async fn handle_text(bot: Bot, msg: Message, state: Arc<AppState>) -> ResponseResult<()> {
    let Some(text) = msg.text() else {
        return Ok(()); // non-text (photo/doc/etc.) — inbound files are #8.
    };
    let chat_id = msg.chat.id.0;

    // Resolve against the *runtime* registry (snapshot under a short read lock),
    // so slots added at runtime via `proxy connect` route too.
    let slots = state.slot_snapshot();
    let Some(slot) = auth::resolve(&slots, chat_id) else {
        // Log the numeric id so the operator can whitelist it (bootstrap aid).
        tracing::warn!(chat_id, "unauthorized sender — not on any slot whitelist");
        bot.send_message(
            msg.chat.id,
            format!(
                "Not authorized. Your chat id is {chat_id} — ask the admin to add it to a slot."
            ),
        )
        .await?;
        return Ok(());
    };

    match run_turn(&state, slot, chat_id, text).await {
        Ok(reply) => {
            let chunks = render::split_message(&reply, TELEGRAM_LIMIT);
            if chunks.is_empty() {
                bot.send_message(msg.chat.id, "(opencode returned no text)")
                    .await?;
            } else {
                for chunk in chunks {
                    bot.send_message(msg.chat.id, chunk).await?;
                }
            }
        }
        Err(err) => {
            tracing::error!(chat_id, slot = %slot.name, error = %err, "turn failed");
            bot.send_message(msg.chat.id, ERROR_REPLY).await?;
        }
    }
    Ok(())
}

/// One blocking turn: resolve/create the session for `chat_id`, send the prompt,
/// return the assistant's visible text. Errors bubble up as `anyhow::Error`.
async fn run_turn(
    state: &AppState,
    slot: &Slot,
    chat_id: i64,
    text: &str,
) -> anyhow::Result<String> {
    // Clone the client out of the registry under a short read lock; the guard is
    // dropped inside `client_for` before any await below.
    let client = state
        .client_for(&slot.name)
        .ok_or_else(|| anyhow::anyhow!("no opencode client for slot '{}'", slot.name))?;

    // Read routing from SQLite (sync, lock released before the await below).
    let stored = state.db.get_session(chat_id)?;
    let session_id = session::get_or_create(
        &client,
        stored.as_deref(),
        &state.cfg.model,
        &state.cfg.permissions.ask,
    )
    .await?;
    // Persist the resolved id so it survives a restart (and a possible recreate).
    state.db.set_session(chat_id, &session_id)?;

    let reply = client
        .prompt(&session_id, PromptModel::from(&state.cfg.model), text)
        .await?;
    Ok(reply.text())
}
