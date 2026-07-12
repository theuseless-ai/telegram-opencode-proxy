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
use std::sync::{Arc, Mutex, PoisonError, RwLock};
use std::time::Duration;

use anyhow::{Context, anyhow};
use teloxide::prelude::*;
use teloxide::types::ChatId;
use teloxide::utils::command::BotCommands;
use tokio::sync::mpsc;
use tracing::Instrument;

use crate::admin::{
    ApproveInfo, BoxFuture, ConnectOutcome, ConnectParams, PairingEntry, SlotInfo,
    SlotInventoryBase,
};
use crate::auth;
use crate::config::{Config, Slot};
use crate::opencode::client::OpencodeClient;
use crate::opencode::types::PromptModel;
use crate::pairing;
use crate::persistence::Db;
use crate::session;
use crate::state::SlotConn;
use crate::telegram::render::Verbosity;
use crate::telegram::{retry, stream};

/// Readiness budget for the interactive `proxy connect` bring-up — short so the
/// command fails fast on an unreachable slot (unlike the 60 s startup budget).
const CONNECT_READY_ATTEMPTS: u32 = 5;
const CONNECT_READY_INTERVAL: Duration = Duration::from_millis(200);

/// Depth of a user's turn queue (#9). Turns run one at a time; up to this many
/// may wait behind the in-flight one before further messages are rejected with
/// [`BUSY_REPLY`] (bounded backpressure — the dispatcher never blocks).
const USER_QUEUE_DEPTH: usize = 8;

/// Reply when a user's turn queue is full (reject-with-message, §6).
const BUSY_REPLY: &str =
    "⏳ I'm still working through your messages — please hold on a moment before sending more.";

/// One queued turn for a user's [serial worker](user_worker): the routed slot and
/// the prompt text. The `chat_id` is fixed per worker, so it isn't carried here.
struct TurnJob {
    slot: Slot,
    text: String,
}

/// Outcome of [`AppState::enqueue_turn`].
enum Enqueue {
    /// Accepted onto the user's queue (a worker was spawned if needed).
    Queued,
    /// The user's queue is full — the caller should reply [`BUSY_REPLY`].
    Full,
}

/// A live per-user turn worker: the bounded channel into it, plus its task
/// handle so [`AppState::shutdown`] can drain it gracefully (#21).
struct Worker {
    tx: mpsc::Sender<TurnJob>,
    handle: tokio::task::JoinHandle<()>,
}

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
    /// Path to the loaded `config.toml`. `proxy connect` writes newly-added slots
    /// back here (format-preserving; #45), so config is the single source of
    /// truth for slots and a `connect`-added seat survives a restart.
    pub config_path: PathBuf,
    pub registry: RwLock<HashMap<String, SlotConn>>,
    pub db: Db,
    /// Bot handle used to notify a user out-of-band — specifically the pairing
    /// approval ping (#4b), sent from the admin-socket handler (which has no
    /// `Message` to reply to). `Bot` is `Arc`-backed, so this clone is cheap.
    pub bot: Bot,
    /// Per-user turn queues (#9): `chat_id → `[`Worker`] (bounded sender + task
    /// handle) for that user's serial worker. A `std::sync::Mutex` — locked only
    /// briefly to look up / insert an entry, never held across an await. Turns for
    /// one user run strictly one at a time; a full queue is rejected, not blocked
    /// (§6). [`shutdown`](Self::shutdown) drains these on exit (#21).
    user_queues: Mutex<HashMap<i64, Worker>>,
    /// Slot names with a background reconnect in flight (#22), so a burst of
    /// turns hitting an unreachable opencode spawns at most one reconnect per slot.
    reconnecting: Mutex<HashSet<String>>,
}

impl AppState {
    /// Build state from config, the config file path (for `proxy connect`
    /// writes; #45), the seeded per-slot registry, an open SQLite handle, and a
    /// bot handle for out-of-band notifications.
    ///
    /// Also **seeds the whitelist** (#4b): every **configured** slot that declares
    /// a `telegram_id` is idempotently written into `allowed_users`, so config,
    /// `proxy connect --telegram-id`, and A4b pairing all share one lookup path —
    /// and a `connect`-added binding survives a restart. Seeding keys off
    /// `cfg.slots`, **not** the registry, so a bound user is authorized whether or
    /// not their opencode has connected yet (slots come up in the background,
    /// #51). Best-effort — a DB hiccup is logged, never fatal.
    pub fn new(
        cfg: Config,
        config_path: PathBuf,
        registry: HashMap<String, SlotConn>,
        db: Db,
        bot: Bot,
    ) -> Arc<Self> {
        for slot in &cfg.slots {
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
            config_path,
            registry: RwLock::new(registry),
            db,
            bot,
            user_queues: Mutex::new(HashMap::new()),
            reconnecting: Mutex::new(HashSet::new()),
        })
    }

    /// Enqueue `job` onto `chat_id`'s serial turn worker, spawning the worker on
    /// first use. Returns [`Enqueue::Full`] when the user's bounded queue is
    /// full (the dispatcher then rejects with [`BUSY_REPLY`] rather than
    /// blocking, §6). `bot` is the dispatcher's handle — the worker replies
    /// through it, so a worker outlives the single update that spawned it.
    fn enqueue_turn(self: &Arc<Self>, bot: &Bot, chat_id: i64, job: TurnJob) -> Enqueue {
        let mut queues = self
            .user_queues
            .lock()
            .unwrap_or_else(PoisonError::into_inner);

        // Fast path: a live worker exists. Recover the job if its channel is
        // full (reject) or closed (worker gone — respawn below).
        let job = match queues.get(&chat_id) {
            Some(worker) => match worker.tx.try_send(job) {
                Ok(()) => return Enqueue::Queued,
                Err(mpsc::error::TrySendError::Full(_)) => return Enqueue::Full,
                Err(mpsc::error::TrySendError::Closed(job)) => {
                    queues.remove(&chat_id);
                    job
                }
            },
            None => job,
        };

        // No (live) worker — create the channel, seat the first job, and spawn.
        let (tx, rx) = mpsc::channel(USER_QUEUE_DEPTH);
        // A fresh channel has capacity ≥ 1, so this send cannot fail.
        let _ = tx.try_send(job);
        let handle = tokio::spawn(user_worker(Arc::clone(self), bot.clone(), chat_id, rx));
        queues.insert(chat_id, Worker { tx, handle });
        Enqueue::Queued
    }

    /// Graceful shutdown (#21): stop the turn workers and flush the store.
    ///
    /// Dropping every worker's sender closes its channel, so each worker finishes
    /// its in-flight turn, drains any queued turns, and exits. We then await the
    /// worker tasks (bounded by `grace`) before checkpointing SQLite. A worker
    /// that overruns `grace` is left to be killed on process exit — best effort.
    pub async fn shutdown(&self, grace: Duration) {
        // Take the workers out under the lock (drops the senders → channels close),
        // then await their handles outside the lock.
        let handles: Vec<tokio::task::JoinHandle<()>> = {
            let mut queues = self
                .user_queues
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            queues.drain().map(|(_, worker)| worker.handle).collect()
        };

        if !handles.is_empty() {
            tracing::info!(
                workers = handles.len(),
                grace_secs = grace.as_secs(),
                "draining in-flight turns"
            );
            let drain = futures_util::future::join_all(handles);
            if tokio::time::timeout(grace, drain).await.is_err() {
                tracing::warn!("shutdown grace elapsed — some turns did not finish");
            }
        }

        // Flush the WAL into the main DB file (best-effort).
        if let Err(err) = self.db.checkpoint() {
            tracing::warn!(error = %err, "WAL checkpoint on shutdown failed");
        }
    }

    /// Best-effort background reconnect of a slot whose opencode a turn just found
    /// unreachable (#22). Idempotent per slot: at most one reconnect runs at a
    /// time (the `reconnecting` guard), so a burst of failed turns doesn't
    /// stampede. A restarted opencode is picked up (and re-validated) for the next
    /// turn; if it's still down the attempt simply logs and clears the guard.
    fn spawn_reconnect(self: &Arc<Self>, slot_name: String) {
        {
            let mut inflight = self
                .reconnecting
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            if !inflight.insert(slot_name.clone()) {
                return; // already reconnecting this slot
            }
        }
        let state = Arc::clone(self);
        tokio::spawn(async move {
            let params = ConnectParams {
                name: slot_name.clone(),
                url: None,
                workdir: None,
                telegram_id: None,
            };
            match state.ensure_connected_impl(params).await {
                Ok(outcome) => {
                    tracing::info!(slot = %slot_name, ?outcome, "reconnected slot after unreachable turn")
                }
                Err(err) => {
                    tracing::warn!(slot = %slot_name, error = format!("{err:#}"), "slot reconnect attempt failed")
                }
            }
            state
                .reconnecting
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .remove(&slot_name);
        });
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

        // Not live in the registry — but a config-declared slot that failed to
        // connect at startup (its opencode wasn't up yet) is still KNOWN. Bring
        // that slot online now (no config write — it's already declared), rather
        // than rejecting it as "does not exist".
        if let Some(slot) = self
            .cfg
            .slots
            .iter()
            .find(|s| s.name == params.name)
            .cloned()
        {
            let conn = crate::bring_up_slot(
                &http,
                &slot,
                &self.cfg.model,
                CONNECT_READY_ATTEMPTS,
                CONNECT_READY_INTERVAL,
            )
            .await
            .with_context(|| format!("connecting config slot '{}'", slot.name))?;
            if let Some(id) = slot.telegram_id {
                self.db.add_allowed(id, &slot.name).with_context(|| {
                    format!("whitelisting telegram_id for slot '{}'", slot.name)
                })?;
            }
            {
                let mut guard = self
                    .registry
                    .write()
                    .unwrap_or_else(PoisonError::into_inner);
                guard.insert(slot.name.clone(), conn);
            }
            return Ok(ConnectOutcome::Connected);
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
        // Persist into config.toml (format-preserving; #45) so it survives a
        // restart — config is the single source of truth for slots. This is
        // best-effort-with-error: a failed write is reported, never silently
        // swallowed, so we don't end up with a registry-only (non-persisted) slot.
        crate::config::upsert_slot(&self.config_path, &slot)
            .with_context(|| format!("persisting slot '{}' to config", slot.name))?;
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
    /// the `allowed_users` bindings. All reads are synchronous; the `connected`
    /// probe is the transport layer's job. Since #45 every slot is config-sourced
    /// (config is the single source of truth), so there is no source tagging.
    fn slot_inventory(&self) -> anyhow::Result<Vec<SlotInventoryBase>> {
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
                    SlotInventoryBase {
                        name: c.slot.name.clone(),
                        opencode_url: c.slot.opencode_url.clone(),
                        workdir: c.slot.workdir.to_string_lossy().into_owned(),
                        telegram_ids,
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

/// Bot commands. `/whoami` aids bootstrap (the operator learns their chat id to
/// whitelist); `/stop` interrupts the in-flight turn (#9); `/new` resets the
/// session and `/quiet` `/verbose` toggle output verbosity (#10). `/get` later.
#[derive(BotCommands, Clone)]
#[command(rename_rule = "lowercase")]
enum Command {
    /// Show your numeric chat id (use it to whitelist yourself in config.toml).
    Whoami,
    /// Stop the turn I'm currently working on.
    Stop,
    /// Start a fresh session (forget the current conversation).
    New,
    /// Toggle quiet mode (answer only, no tool status).
    Quiet,
    /// Toggle verbose mode (extra detail).
    Verbose,
}

/// Friendly message shown to the user when a turn fails; details go to the log.
const ERROR_REPLY: &str = "⚠️ Sorry — something went wrong answering that. Please try again.";

/// Shown when the turn failed specifically because the user's opencode instance
/// was unreachable (#22) — distinct from a generic failure, and actionable.
const OPENCODE_UNREACHABLE_REPLY: &str = "🔌 Your opencode instance looks unreachable right now. I'm trying to reconnect — \
     please resend your message in a moment.";

/// Whether `err`'s cause chain contains a connection/timeout failure — i.e. the
/// opencode instance was unreachable, rather than an application-level error
/// (#22). Drives the user-facing message and the background reconnect.
fn is_opencode_unreachable(err: &anyhow::Error) -> bool {
    err.chain().any(|cause| {
        cause
            .downcast_ref::<reqwest::Error>()
            .is_some_and(|re| re.is_connect() || re.is_timeout())
    })
}

/// Run the long-poll dispatcher until a shutdown signal. Returns when the
/// dispatcher has stopped polling (SIGINT via the built-in Ctrl-C handler, or
/// SIGTERM via [`shutdown_on_sigterm`]); the caller then drains workers and
/// flushes the store ([`AppState::shutdown`], #21). `bot` and `state` are
/// injected as handler dependencies.
pub async fn run(bot: Bot, state: Arc<AppState>) {
    // Advertise exactly the bot's own commands (`/whoami`, `/stop`). This
    // replaces any stale menu previously registered via @BotFather, keeping
    // Telegram's "/" command list in sync with the code. Best-effort — a failure
    // here must not stop the dispatcher.
    if let Err(err) = bot.set_my_commands(Command::bot_commands()).await {
        tracing::warn!(error = %err, "could not set the bot command menu");
    }

    let handler = Update::filter_message()
        .branch(teloxide::filter_command::<Command, _>().endpoint(handle_command))
        .branch(dptree::endpoint(handle_text));

    let mut dispatcher = Dispatcher::builder(bot, handler)
        .dependencies(dptree::deps![state])
        .enable_ctrlc_handler() // SIGINT / Ctrl-C
        .build();

    // Also stop gracefully on SIGTERM (e.g. `systemctl stop`), by triggering the
    // same dispatcher shutdown the Ctrl-C handler uses.
    shutdown_on_sigterm(dispatcher.shutdown_token());

    dispatcher.dispatch().await;
}

/// Spawn a task that triggers the dispatcher's graceful shutdown on the first
/// `SIGTERM`. Unix-only; on other targets it's a no-op (Ctrl-C still applies).
#[cfg(unix)]
fn shutdown_on_sigterm(token: teloxide::dispatching::ShutdownToken) {
    tokio::spawn(async move {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut sigterm) => {
                sigterm.recv().await;
                tracing::info!("received SIGTERM — shutting down");
                // Errors only if already idle/shutting down — nothing to do then.
                let _ = token.shutdown();
            }
            Err(err) => tracing::warn!(error = %err, "could not install SIGTERM handler"),
        }
    });
}

#[cfg(not(unix))]
fn shutdown_on_sigterm(_token: teloxide::dispatching::ShutdownToken) {}

/// Handle a slash command. `/whoami` is deliberately NOT gated by auth — an
/// unwhitelisted user needs their chat id to get whitelisted. `/stop` resolves
/// the user's session and aborts its in-flight turn.
async fn handle_command(
    bot: Bot,
    msg: Message,
    cmd: Command,
    state: Arc<AppState>,
) -> ResponseResult<()> {
    match cmd {
        Command::Whoami => {
            let text = format!("Your chat id is `{}`.", msg.chat.id.0);
            bot.send_message(msg.chat.id, text).await?;
            Ok(())
        }
        Command::Stop => handle_stop(bot, msg, state).await,
        Command::New => handle_new(bot, msg, state).await,
        Command::Quiet => handle_verbosity(bot, msg, state, Verbosity::Quiet).await,
        Command::Verbose => handle_verbosity(bot, msg, state, Verbosity::Verbose).await,
    }
}

/// `/new` (#10): forget the user's current opencode session so the next message
/// starts fresh. Clearing a routing row that isn't there is harmless, so this
/// isn't auth-gated.
pub async fn handle_new(bot: Bot, msg: Message, state: Arc<AppState>) -> ResponseResult<()> {
    let chat_id = msg.chat.id.0;
    match state.db.clear_session(chat_id) {
        Ok(()) => {
            bot.send_message(msg.chat.id, "🆕 Started a fresh session.")
                .await?;
        }
        Err(err) => {
            tracing::error!(chat_id, error = %err, "clearing session on /new failed");
            bot.send_message(
                msg.chat.id,
                "⚠️ Couldn't start a new session — please try again.",
            )
            .await?;
        }
    }
    Ok(())
}

/// `/quiet` · `/verbose` (#10): toggle the requested verbosity. Requesting the
/// level you're already at returns to `Normal`, so each command is its own
/// on/off switch (and there's always a way back to the default).
pub async fn handle_verbosity(
    bot: Bot,
    msg: Message,
    state: Arc<AppState>,
    requested: Verbosity,
) -> ResponseResult<()> {
    let chat_id = msg.chat.id.0;
    let current = state.db.get_verbosity(chat_id).unwrap_or_default();
    let next = if current == requested {
        Verbosity::Normal
    } else {
        requested
    };

    if let Err(err) = state.db.set_verbosity(chat_id, next) {
        tracing::error!(chat_id, error = %err, "setting verbosity failed");
        bot.send_message(
            msg.chat.id,
            "⚠️ Couldn't change verbosity — please try again.",
        )
        .await?;
        return Ok(());
    }

    let note = match next {
        Verbosity::Quiet => "🔕 Quiet mode: I'll show just the answer.",
        Verbosity::Normal => "🔔 Normal mode: answer plus a tool-status line.",
        Verbosity::Verbose => "🔊 Verbose mode on.",
    };
    bot.send_message(msg.chat.id, note).await?;
    Ok(())
}

/// `/stop` (#9): abort the user's in-flight opencode turn via
/// `POST /session/:id/abort`, which unblocks its running `prompt`. A sender with
/// no slot binding or no live session is simply told there's nothing to stop.
///
/// `pub` so the harness can drive it directly (like [`handle_text`]).
pub async fn handle_stop(bot: Bot, msg: Message, state: Arc<AppState>) -> ResponseResult<()> {
    const NOTHING: &str = "Nothing to stop.";
    let chat_id = msg.chat.id.0;

    // Resolve the user's slot (short registry snapshot; auth is DB-backed).
    let slots = state.slot_snapshot();
    let slot = match auth::resolve(&state.db, &slots, chat_id) {
        Ok(Some(slot)) => slot,
        Ok(None) => {
            bot.send_message(msg.chat.id, NOTHING).await?;
            return Ok(());
        }
        Err(err) => {
            tracing::error!(chat_id, error = %err, "auth lookup failed on /stop");
            bot.send_message(msg.chat.id, NOTHING).await?;
            return Ok(());
        }
    };

    // TODO(#13): also reject any permission request this user has pending.

    let session_id = match state.db.get_session(chat_id) {
        Ok(Some(id)) => id,
        Ok(None) => {
            bot.send_message(msg.chat.id, NOTHING).await?;
            return Ok(());
        }
        Err(err) => {
            tracing::error!(chat_id, error = %err, "reading session on /stop");
            bot.send_message(msg.chat.id, ERROR_REPLY).await?;
            return Ok(());
        }
    };

    let Some(client) = state.client_for(&slot.name) else {
        bot.send_message(msg.chat.id, NOTHING).await?;
        return Ok(());
    };

    match client.abort_session(&session_id).await {
        Ok(_) => {
            bot.send_message(msg.chat.id, "🛑 Stopped.").await?;
        }
        Err(err) => {
            tracing::error!(chat_id, session_id, error = %err, "aborting session failed");
            bot.send_message(msg.chat.id, "⚠️ Couldn't stop the current turn.")
                .await?;
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

    // Hand the turn to the user's serial worker (#9): turns for one user run one
    // at a time, and a full queue is rejected here rather than blocking the
    // dispatcher. The reply is produced by the worker, through `bot`.
    let job = TurnJob {
        slot,
        text: text.to_string(),
    };
    match state.enqueue_turn(&bot, chat_id, job) {
        Enqueue::Queued => {}
        Enqueue::Full => {
            let chat = msg.chat.id;
            retry::with_retry("busy_reply", || {
                let bot = bot.clone();
                async move { bot.send_message(chat, BUSY_REPLY).await }
            })
            .await?;
        }
    }
    Ok(())
}

/// The per-turn tracing span (#26): correlates every log line emitted while a
/// turn runs by `chat_id` and `slot`; the resolved opencode `session` is recorded
/// onto it once known (see [`run_turn`]). Deliberately carries **no** message
/// content — user text is never logged (redaction; the token is a `Secret`, #23).
fn turn_span(chat_id: i64, slot: &str) -> tracing::Span {
    tracing::info_span!(
        "turn",
        chat_id,
        slot = %slot,
        session = tracing::field::Empty,
    )
}

/// A user's serial turn worker (#9): drains the bounded queue, running each turn
/// to completion before the next so a single user's turns never interleave.
/// Errors become the friendly [`ERROR_REPLY`] plus a `tracing::error!`; the loop
/// ends (and the worker is dropped) only if the queue is closed. Each turn runs
/// inside a [`turn_span`] so its logs are correlated (#26).
async fn user_worker(
    state: Arc<AppState>,
    bot: Bot,
    chat_id: i64,
    mut rx: mpsc::Receiver<TurnJob>,
) {
    while let Some(job) = rx.recv().await {
        let span = turn_span(chat_id, &job.slot.name);
        let result = run_turn(&bot, &state, &job.slot, chat_id, &job.text)
            .instrument(span)
            .await;
        if let Err(err) = result {
            // Distinguish "your opencode is unreachable" (actionable, and worth a
            // background reconnect) from a generic failure (#22).
            let reply = if is_opencode_unreachable(&err) {
                tracing::warn!(chat_id, slot = %job.slot.name, error = format!("{err:#}"), "turn failed — opencode unreachable");
                state.spawn_reconnect(job.slot.name.clone());
                OPENCODE_UNREACHABLE_REPLY
            } else {
                tracing::error!(chat_id, slot = %job.slot.name, error = format!("{err:#}"), "turn failed");
                ERROR_REPLY
            };
            let chat = ChatId(chat_id);
            let _ = retry::with_retry("error_reply", || {
                let bot = bot.clone();
                async move { bot.send_message(chat, reply).await }
            })
            .await;
        }
    }
    tracing::debug!(chat_id, "user turn worker stopped (queue closed)");
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

/// One streaming turn: resolve/create the session for `chat_id`, then hand off
/// to [`stream::run_streaming_turn`], which fires the blocking prompt and renders
/// live (deltas → throttled edits, `typing` liveness, tool status) until the
/// assistant message is finalized. Errors bubble up as `anyhow::Error`.
async fn run_turn(
    bot: &Bot,
    state: &AppState,
    slot: &Slot,
    chat_id: i64,
    text: &str,
) -> anyhow::Result<()> {
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
    // Correlate the rest of this turn's logs with the resolved session (#26).
    tracing::Span::current().record("session", session_id.as_str());

    // The user's output verbosity (#10) — defaults to Normal; a DB hiccup here
    // must not fail the turn, so fall back to the default.
    let verbosity = state.db.get_verbosity(chat_id).unwrap_or_default();

    // A short-lived HTTP client for this turn's `/global/event` subscription.
    let http = reqwest::Client::new();
    stream::run_streaming_turn(
        bot,
        &http,
        &client,
        &slot.opencode_url,
        chat_id,
        &session_id,
        PromptModel::from(&state.cfg.model),
        text,
        verbosity,
        stream::StreamTiming::default(),
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn connection_refused_classifies_as_opencode_unreachable() {
        // Port 1 refuses immediately → a reqwest connect error, wrapped the same
        // way the turn path wraps it (`.context(...)`).
        let http = reqwest::Client::builder()
            .connect_timeout(Duration::from_millis(200))
            .build()
            .expect("client");
        let err = http
            .get("http://127.0.0.1:1/config")
            .send()
            .await
            .expect_err("connection to :1 must fail");
        let wrapped = anyhow::Error::new(err).context("GET /config");
        assert!(
            is_opencode_unreachable(&wrapped),
            "a connect failure must be classified as unreachable"
        );
    }

    #[test]
    fn application_errors_are_not_unreachable() {
        let err = anyhow::anyhow!("model 'ghost' is not configured under provider 'x'");
        assert!(
            !is_opencode_unreachable(&err),
            "a non-network error must not be treated as unreachable"
        );
    }

    /// The per-turn span (#26) surfaces the structured `chat_id`/`slot` fields on
    /// events emitted within it — and never any message content.
    #[test]
    fn turn_span_carries_structured_fields() {
        use std::io;
        use std::sync::{Arc, Mutex};

        #[derive(Clone)]
        struct Capture(Arc<Mutex<Vec<u8>>>);
        impl io::Write for Capture {
            fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(buf);
                Ok(buf.len())
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        impl tracing_subscriber::fmt::MakeWriter<'_> for Capture {
            type Writer = Capture;
            fn make_writer(&self) -> Self::Writer {
                self.clone()
            }
        }

        let sink = Capture(Arc::new(Mutex::new(Vec::new())));
        let subscriber = tracing_subscriber::fmt()
            .with_writer(sink.clone())
            .with_ansi(false)
            .finish();

        tracing::subscriber::with_default(subscriber, || {
            let span = turn_span(4242, "you");
            let _entered = span.enter();
            tracing::info!("turn started");
        });

        let out = String::from_utf8(sink.0.lock().unwrap().clone()).unwrap();
        assert!(out.contains("turn"), "span name present: {out}");
        assert!(out.contains("chat_id=4242"), "chat_id field present: {out}");
        assert!(out.contains("slot=you"), "slot field present: {out}");
    }
}
