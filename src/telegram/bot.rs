//! teloxide long-poll dispatcher: text messages, `/new` `/whoami` `/get` `/stop`,
//! `callback_query` (approval buttons), inbound file download. Whitelist gate.
//! See `docs/design/architecture.md` §4/§13. Issue #6.
//!
//! #6 wires the "wire green" subset: a text message from a whitelisted user is
//! routed to that user's opencode slot (blocking `POST /session/:id/message`)
//! and the reply is chunked back with [`render::split_message`]. `/whoami`
//! reports the numeric chat id (a bootstrap aid). Streaming, files, `/new`,
//! `/get`, `/stop`, and per-user mpsc serialization land in later issues.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, PoisonError, RwLock};
use std::time::Duration;

use anyhow::{Context, anyhow};
use teloxide::prelude::*;
use teloxide::types::ChatId;
use teloxide::utils::command::BotCommands;

use crate::admin::{
    ApproveInfo, BoxFuture, ConnectOutcome, ConnectParams, PairingEntry, SlotInfo,
    SlotInventoryBase, SlotSource,
};
use crate::auth;
use crate::config::{Config, Slot};
use crate::opencode::client::OpencodeClient;
use crate::opencode::types::PromptModel;
use crate::pairing;
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
    /// Bot handle used to notify a user out-of-band — specifically the pairing
    /// approval ping (#4b), sent from the admin-socket handler (which has no
    /// `Message` to reply to). `Bot` is `Arc`-backed, so this clone is cheap.
    pub bot: Bot,
}

impl AppState {
    /// Build state from config, the seeded per-slot registry (config ∪ DB), an
    /// open SQLite handle, and a bot handle for out-of-band notifications.
    ///
    /// Also **seeds the whitelist** (#4b): every slot in the registry
    /// (config ∪ DB) that declares a `telegram_id` is idempotently written into
    /// `allowed_users`, so config, `proxy connect --telegram-id`, and A4b pairing
    /// all share one lookup path — and a `connect`-added binding survives a
    /// restart. Seeding is best-effort — a DB hiccup is logged, never fatal.
    pub fn new(cfg: Config, registry: HashMap<String, SlotConn>, db: Db, bot: Bot) -> Arc<Self> {
        for conn in registry.values() {
            let slot = &conn.slot;
            if let Some(id) = slot.telegram_id
                && let Err(err) = db.add_allowed(id, &slot.name)
            {
                tracing::warn!(
                    chat_id = id,
                    slot = %slot.name,
                    error = %err,
                    "failed to seed telegram_id into allowed_users"
                );
            }
        }
        Arc::new(Self {
            cfg,
            registry: RwLock::new(registry),
            db,
            bot,
        })
    }

    /// The names of every live slot in the registry (short read guard, dropped
    /// before returning).
    fn registry_names(&self) -> Vec<String> {
        let guard = self.registry.read().unwrap_or_else(PoisonError::into_inner);
        guard.values().map(|c| c.slot.name.clone()).collect()
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
        // Whitelist the bound user now (if declared) — auth reads allowed_users,
        // so a `--telegram-id` add takes effect immediately, like config/pairing.
        if let Some(id) = slot.telegram_id {
            self.db
                .add_allowed(id, &slot.name)
                .with_context(|| format!("whitelisting telegram_id for slot '{}'", slot.name))?;
        }
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

    /// Per-slot inventory (#4b): registry slots (name/url/workdir) folded with
    /// the `allowed_users` bindings and tagged config- vs db-sourced. All reads
    /// are synchronous; the `connected` probe is the transport layer's job.
    fn slot_inventory(&self) -> anyhow::Result<Vec<SlotInventoryBase>> {
        let config_names: HashSet<&str> = self.cfg.slots.iter().map(|s| s.name.as_str()).collect();
        // (chat_id, slot) bindings grouped per slot.
        let allowed = self.db.list_allowed()?;

        let mut out: Vec<SlotInventoryBase> = {
            let guard = self.registry.read().unwrap_or_else(PoisonError::into_inner);
            guard
                .values()
                .map(|c| {
                    let telegram_ids = allowed
                        .iter()
                        .filter(|(_, slot)| slot == &c.slot.name)
                        .map(|(chat_id, _)| *chat_id)
                        .collect();
                    let source = if config_names.contains(c.slot.name.as_str()) {
                        SlotSource::Config
                    } else {
                        SlotSource::Db
                    };
                    SlotInventoryBase {
                        name: c.slot.name.clone(),
                        opencode_url: c.slot.opencode_url.clone(),
                        workdir: c.slot.workdir.to_string_lossy().into_owned(),
                        telegram_ids,
                        source,
                    }
                })
                .collect()
        };
        out.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(out)
    }

    /// The live pending pairings (#4b), each stamped with its age (derived from
    /// `expires_at` and the configured TTL, since only expiry is persisted).
    fn pair_list(&self) -> anyhow::Result<Vec<PairingEntry>> {
        let now = pairing::now_epoch();
        let ttl = self.cfg.pairing.code_ttl_secs;
        let pending = pairing::list(&self.db, now)?;
        Ok(pending
            .into_iter()
            .map(|p| {
                let issued_at = p.expires_at.saturating_sub(ttl);
                PairingEntry {
                    code: p.code,
                    username: p.username,
                    chat_id: p.chat_id,
                    age_secs: now.saturating_sub(issued_at).max(0),
                }
            })
            .collect())
    }

    fn pair_approve<'a>(
        &'a self,
        code: String,
        slot: String,
    ) -> BoxFuture<'a, anyhow::Result<ApproveInfo>> {
        Box::pin(async move {
            // Bind synchronously (no lock held across the await below), then
            // notify the freshly-paired user via the bot.
            let names = self.registry_names();
            let outcome = pairing::approve(&self.db, &names, &code, &slot, pairing::now_epoch())?;

            let text = format!(
                "✅ Approved! You're now paired to slot '{}'. Send me a message to get started.",
                outcome.slot
            );
            self.bot
                .send_message(ChatId(outcome.chat_id), text)
                .await
                .with_context(|| format!("notifying paired user {}", outcome.chat_id))?;

            Ok(ApproveInfo {
                chat_id: outcome.chat_id,
                slot: outcome.slot,
                username: outcome.username,
            })
        })
    }

    fn pair_deny(&self, code: String) -> anyhow::Result<bool> {
        pairing::deny(&self.db, &code)
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

    // Resolve against the persisted whitelist, mapping the bound slot name onto
    // the *runtime* registry snapshot (short read lock) so runtime-added slots
    // route too. A DB error here is treated as "unresolved" and reported.
    let slots = state.slot_snapshot();
    let resolved = match auth::resolve(&state.db, &slots, chat_id) {
        Ok(resolved) => resolved,
        Err(err) => {
            tracing::error!(chat_id, error = %err, "auth lookup failed");
            bot.send_message(msg.chat.id, ERROR_REPLY).await?;
            return Ok(());
        }
    };
    let Some(slot) = resolved else {
        handle_unauthorized(&bot, &msg, &state, chat_id).await?;
        return Ok(());
    };

    match run_turn(&state, &slot, chat_id, text).await {
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

/// The unauthorized branch of [`handle_text`] (#4b): issue a single-use pairing
/// code and tell the user how to get it approved. Rate-limited/idempotent per
/// chat by [`pairing::issue_code`]. On a code-issue failure it never panics — it
/// falls back to a plain rejection and logs the error.
async fn handle_unauthorized(
    bot: &Bot,
    msg: &Message,
    state: &AppState,
    chat_id: i64,
) -> ResponseResult<()> {
    let username = msg.from.as_ref().and_then(|u| u.username.clone());
    let ttl = state.cfg.pairing.code_ttl_secs;

    match pairing::issue_code(
        &state.db,
        chat_id,
        username.as_deref(),
        ttl,
        pairing::now_epoch(),
    ) {
        Ok(code) => {
            let mins = (ttl / 60).max(1);
            tracing::info!(chat_id, "issued pairing code to unauthorized sender");
            bot.send_message(
                msg.chat.id,
                format!(
                    "Not authorized. Your pairing code is {code} (expires in {mins} min). \
                     Ask the admin to run: proxy pair approve {code} --slot <name>."
                ),
            )
            .await?;
        }
        Err(err) => {
            tracing::error!(chat_id, error = %err, "failed to issue pairing code");
            bot.send_message(
                msg.chat.id,
                "Not authorized, and I couldn't issue a pairing code right now. \
                 Please try again in a moment.",
            )
            .await?;
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
