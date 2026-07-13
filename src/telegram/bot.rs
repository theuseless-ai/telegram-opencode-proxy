//! teloxide long-poll dispatcher: text + media messages, `/whoami` `/stop` `/new`
//! `/quiet` `/verbose` `/get`, and the whitelist gate. See `architecture.md` §4/§13.
//!
//! A whitelisted user's text is routed to their opencode slot and answered by a
//! streaming turn (#8) run through a per-user serial worker (#9); an inbound
//! photo/document is downloaded and attached as a file part (#11, `handle_media`).
//! `/get <path>` sends a workdir file back out (#12, `handle_get`); the outbox
//! watcher (`outbox.rs`) pushes files the agent writes. The permission relay is
//! the inline-button callback path (#13, `handle_callback`).

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, PoisonError, RwLock};
use std::time::Duration;

use anyhow::{Context, anyhow};
use teloxide::prelude::*;
use teloxide::types::{CallbackQuery, ChatId};
use teloxide::utils::command::BotCommands;
use tokio::sync::mpsc;
use tracing::Instrument;

use crate::admin::{
    ApproveInfo, BoxFuture, ConnectOutcome, ConnectParams, PairingEntry, SlotInfo,
    SlotInventoryBase,
};
use crate::auth;
use crate::config::{Config, Slot};
use crate::mcp::store::FileStore;
use crate::opencode::client::OpencodeClient;
use crate::opencode::types::{PartInput, PermissionReplyRequest, PromptModel};
use crate::pairing;
use crate::permission;
use crate::persistence::Db;
use crate::session;
use crate::state::SlotConn;
use crate::telegram::render::Verbosity;
use crate::telegram::{files, retry, stream};

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
/// the prompt parts (text and/or inbound files, #11). The `chat_id` is fixed per
/// worker, so it isn't carried here.
struct TurnJob {
    slot: Slot,
    parts: Vec<PartInput>,
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
    /// The shared, disk-backed file store behind the MCP file-transfer feature
    /// (#65). Both the inbound-media path (which `put`s a downloaded file and
    /// announces its download URL to the model) and the `GET /files/{id}` endpoint
    /// (which `take_by_id`s it) reach it through this `Arc`. Built once here from
    /// the `[mcp]` config so every `AppState::new` caller shares one store; the TTL
    /// sweep is spawned separately in `serve()` (#65 T7).
    pub file_store: Arc<FileStore>,
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
        // Build the shared MCP file store from the `[mcp]` config before `cfg` is
        // moved into the struct. Constructed here (not passed in) so the several
        // callers of `AppState::new` keep a stable signature.
        let file_store = Arc::new(FileStore::new(
            cfg.mcp.max_file_bytes,
            Duration::from_secs(cfg.mcp.ttl_secs),
        ));
        Arc::new(Self {
            cfg,
            config_path,
            registry: RwLock::new(registry),
            db,
            bot,
            user_queues: Mutex::new(HashMap::new()),
            reconnecting: Mutex::new(HashSet::new()),
            file_store,
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
    pub(crate) fn slot_snapshot(&self) -> Vec<Slot> {
        let guard = self.registry.read().unwrap_or_else(PoisonError::into_inner);
        guard.values().map(|c| c.slot.clone()).collect()
    }

    /// Clone the ready client for `name` out of the registry (guard dropped
    /// before the caller awaits), or `None` if the slot is unknown.
    pub(crate) fn client_for(&self, name: &str) -> Option<OpencodeClient> {
        let guard = self.registry.read().unwrap_or_else(PoisonError::into_inner);
        guard.get(name).map(|c| c.client.clone())
    }

    /// The resolved context-window size for slot `name` (#72), or `None` when the
    /// slot is unknown or no limit could be resolved.
    pub(crate) fn context_limit_for(&self, name: &str) -> Option<u64> {
        let guard = self.registry.read().unwrap_or_else(PoisonError::into_inner);
        guard.get(name).and_then(|c| c.context_limit)
    }

    /// The model selector resolved for slot `name` at connect (#74), or `None`
    /// when the slot is unknown. Cloned out under a short read guard.
    pub(crate) fn model_for(&self, name: &str) -> Option<crate::config::Model> {
        let guard = self.registry.read().unwrap_or_else(PoisonError::into_inner);
        guard.get(name).map(|c| c.model.clone())
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
                self.cfg.model.as_ref(),
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
                self.cfg.model.as_ref(),
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
            self.cfg.model.as_ref(),
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
    /// Send me a file from your workdir: `/get <path>`.
    Get(String),
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

    let handler = dptree::entry()
        .branch(
            Update::filter_message()
                .branch(teloxide::filter_command::<Command, _>().endpoint(handle_command))
                // Text (non-command) → the text turn; everything else
                // (photo/document) → media, which downloads inbound files (#11).
                .branch(dptree::filter(|msg: Message| msg.text().is_some()).endpoint(handle_text))
                .branch(dptree::endpoint(handle_media)),
        )
        // Inline-button taps → the permission relay (#13).
        .branch(Update::filter_callback_query().endpoint(handle_callback));

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
        Command::Get(path) => handle_get(bot, msg, state, path).await,
    }
}

/// Handle an inline-button tap — the permission relay (#13). Parses the callback
/// data, looks up the stored gate, replies to opencode (`reply_permission`,
/// which unblocks the held turn), deletes the gate, and replaces the buttons
/// with the decision. Always answers the callback so the client stops spinning.
///
/// `pub` so the harness can drive it directly (like [`handle_text`]).
pub async fn handle_callback(
    bot: Bot,
    query: CallbackQuery,
    state: Arc<AppState>,
) -> ResponseResult<()> {
    let data = query.data.clone().unwrap_or_default();
    let Some((token, reply)) = permission::parse_callback(&data) else {
        bot.answer_callback_query(query.id).await?; // not ours — just ack.
        return Ok(());
    };

    let approval = match state.db.approval(&token) {
        Ok(Some(approval)) => approval,
        Ok(None) => {
            bot.answer_callback_query(query.id)
                .text("This request has expired.")
                .await?;
            return Ok(());
        }
        Err(err) => {
            tracing::error!(error = %err, "reading approval failed");
            bot.answer_callback_query(query.id)
                .text("Something went wrong.")
                .await?;
            return Ok(());
        }
    };

    // Resolve the slot + client that owns the gate.
    let slots = state.slot_snapshot();
    let client = auth::resolve(&state.db, &slots, approval.chat_id)
        .ok()
        .flatten()
        .and_then(|slot| state.client_for(&slot.name));
    let Some(client) = client else {
        bot.answer_callback_query(query.id)
            .text("Can't reach opencode.")
            .await?;
        return Ok(());
    };

    let permission_id = approval
        .permission_id
        .clone()
        .unwrap_or_else(|| token.clone());
    let result = client
        .reply_permission(
            &permission_id,
            PermissionReplyRequest {
                reply,
                message: None,
            },
        )
        .await;
    let _ = state.db.delete_approval(&token);

    match result {
        Ok(()) => {
            // Replace the buttons with the decision (best-effort).
            if let Some(msg) = &query.message {
                let _ = bot
                    .edit_message_text(
                        ChatId(approval.chat_id),
                        msg.id(),
                        permission::decision_text(reply),
                    )
                    .await;
            }
            bot.answer_callback_query(query.id).await?;
        }
        Err(err) => {
            tracing::error!(chat_id = approval.chat_id, error = %err, "reply_permission failed");
            bot.answer_callback_query(query.id)
                .text("Failed to send your decision.")
                .await?;
        }
    }
    Ok(())
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

/// `/get <path>` (#12): send the user a file from their slot's workdir. The path
/// is resolved and **guarded** by [`files::resolve_within_workdir`] — a `../`
/// traversal, an absolute path elsewhere, or a symlink out of the workdir is
/// rejected before anything is read. Auth-gated: only a bound user (whose slot is
/// live) has a workdir to read from.
///
/// `pub` so the harness can drive it directly (like [`handle_stop`]).
pub async fn handle_get(
    bot: Bot,
    msg: Message,
    state: Arc<AppState>,
    path: String,
) -> ResponseResult<()> {
    let chat_id = msg.chat.id.0;
    let requested = path.trim();
    if requested.is_empty() {
        bot.send_message(
            msg.chat.id,
            "Usage: `/get <path>` — a file in your workdir.",
        )
        .await?;
        return Ok(());
    }

    // Auth-gate: resolve the sender's live slot (its workdir is the read root).
    let slots = state.slot_snapshot();
    let slot = match auth::resolve(&state.db, &slots, chat_id) {
        Ok(Some(slot)) => slot,
        Ok(None) => {
            bot.send_message(msg.chat.id, "You're not set up to fetch files yet.")
                .await?;
            return Ok(());
        }
        Err(err) => {
            tracing::error!(chat_id, error = %err, "auth lookup failed on /get");
            bot.send_message(msg.chat.id, ERROR_REPLY).await?;
            return Ok(());
        }
    };

    // Guard the requested path against the workdir before touching disk.
    let resolved = match files::resolve_within_workdir(&slot.workdir, requested) {
        Ok(resolved) => resolved,
        Err(err) => {
            tracing::info!(chat_id, slot = %slot.name, requested, error = %err, "rejected /get path");
            bot.send_message(msg.chat.id, format!("⚠️ Can't get `{requested}`: {err}"))
                .await?;
            return Ok(());
        }
    };

    if let Err(err) = files::send_outbound_file(&bot, msg.chat.id, &resolved).await {
        tracing::error!(
            chat_id,
            slot = %slot.name,
            path = %resolved.display(),
            error = format!("{err:#}"),
            "sending /get file failed"
        );
        bot.send_message(
            msg.chat.id,
            "⚠️ Couldn't send that file — please try again.",
        )
        .await?;
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
        return Ok(()); // non-text is routed to `handle_media`.
    };
    let chat_id = msg.chat.id.0;
    if let Some(slot) = resolve_or_reject(&bot, &msg, &state, chat_id).await? {
        let parts = vec![PartInput::Text {
            text: text.to_string(),
        }];
        enqueue_parts(&bot, &msg, &state, chat_id, slot, parts).await?;
    }
    Ok(())
}

/// Handle a media message — an inbound photo or document. Non-file, non-text
/// messages are ignored.
///
/// Two mutually-exclusive inbound paths, gated by config (both-on would double the
/// file into the turn context):
/// - **MCP announce (#65), the default** when `mcp.enabled && !mcp.filepart_fallback`:
///   stash the bytes in the [`FileStore`](crate::mcp::store::FileStore) under the
///   sender's slot and inject an imperative announce text part carrying a one-shot
///   download URL the model `curl`s and reads with its own tools. See
///   [`handle_media_announce`].
/// - **FilePart fallback (#11)** when `mcp.filepart_fallback` is set **or** MCP is
///   disabled: base64-encode the file as a data-URI [`PartInput::File`] inline in
///   the turn, unchanged from #11. See [`handle_media_filepart`].
///
/// `pub` so the harness can drive it directly (like [`handle_text`]).
pub async fn handle_media(bot: Bot, msg: Message, state: Arc<AppState>) -> ResponseResult<()> {
    let chat_id = msg.chat.id.0;
    let Some(slot) = resolve_or_reject(&bot, &msg, &state, chat_id).await? else {
        return Ok(());
    };
    if state.cfg.mcp.filepart_fallback || !state.cfg.mcp.enabled {
        handle_media_filepart(bot, msg, state, chat_id, slot).await
    } else {
        handle_media_announce(bot, msg, state, chat_id, slot).await
    }
}

/// The MCP announce inbound path (#65, default): download the file, stash it in
/// the [`FileStore`](crate::mcp::store::FileStore) under `slot`, and enqueue an
/// imperative announce text part carrying a one-shot `GET /files/{id}` download
/// URL (plus any caption as its own text part) so the model `curl`s the file into
/// its workspace and reads it with its own tools. A download or store failure
/// becomes the friendly [`reply_inbound_file_failed`] reply plus a `tracing::warn!`.
async fn handle_media_announce(
    bot: Bot,
    msg: Message,
    state: Arc<AppState>,
    chat_id: i64,
    slot: Slot,
) -> ResponseResult<()> {
    let (filename, mime, bytes) = match files::download_inbound(&bot, &msg).await {
        Ok(Some(triple)) => triple,
        Ok(None) => return Ok(()), // not a photo/document — nothing to do.
        Err(err) => {
            tracing::warn!(chat_id, slot = %slot.name, error = format!("{err:#}"), "inbound file download failed");
            return reply_inbound_file_failed(&bot, &msg).await;
        }
    };

    let id = match state
        .file_store
        .put(&slot.name, &filename, &mime, &bytes[..])
        .await
    {
        Ok(id) => id,
        Err(err) => {
            tracing::warn!(chat_id, slot = %slot.name, error = %err, "storing inbound file failed");
            return reply_inbound_file_failed(&bot, &msg).await;
        }
    };

    // Imperative, self-describing announce — the model downloads the file INTO ITS
    // WORKSPACE with a one-shot URL, then opens it with its own tools (format-
    // agnostic; PDF/image/…). The wording deliberately frames it as "download a
    // file, then open it" and counters the model's reflexive "I can't view images"
    // refusal — opencode's read tool renders images and extracts document text from
    // a file on disk, so the model must open the file rather than assume it can't.
    let url = state.cfg.mcp.download_url(&id);
    // Land inbound files in a `downloads/` folder so the workspace stays tidy; the
    // model creates it if missing (`mkdir -p`).
    let path = format!("downloads/{filename}");
    // The verb matters: models reliably act on "view the image" for pictures and
    // "extract the text" for documents, but reflexively refuse "read this PDF" (a
    // text+image model genuinely can't ingest a PDF; extracting its text is the
    // path that works). So step 2 is tailored to the file type — still one download.
    let read_step = if mime.starts_with("image/") {
        format!(
            "2. Open and VIEW the image `{path}` with your file-reading tool — it displays images from disk, so this is how you see the photo."
        )
    } else {
        format!(
            "2. Open `{path}` and EXTRACT ITS TEXT with your file-reading tool — it extracts the text of PDFs and documents from disk. You do NOT need native PDF support: extracting the text IS how you read the file."
        )
    };
    // Fold any caption INTO the instruction as the question to answer, so it can't
    // compete with the file-handling steps as a separate message (a caption like
    // "what's in this pdf?" otherwise triggers the model's refusal reflex).
    let answer_step = match msg.caption().map(str::trim).filter(|c| !c.is_empty()) {
        Some(caption) => {
            format!("3. Then answer the user's question using the file. They asked: \"{caption}\".")
        }
        None => "3. Then tell the user what the file contains.".to_string(),
    };
    let parts = vec![PartInput::Text {
        text: format!(
            "The user has sent you a file named `{filename}`. You MUST do ALL of these steps, in order, before replying:\n\
             1. Download it into a `downloads` folder in your workspace (create the folder if it doesn't exist): run `mkdir -p downloads && curl -sf '{url}' -o '{path}'` (this link works only once).\n\
             {read_step}\n\
             {answer_step}\n\
             Do NOT stop after downloading and do NOT refuse — for a PDF or document EXTRACT ITS TEXT, for an image VIEW it, then reply."
        ),
    }];
    enqueue_parts(&bot, &msg, &state, chat_id, slot, parts).await
}

/// The #11 FilePart inbound path (fallback): download the file, base64-encode it
/// as a data-URI [`PartInput::File`], append any caption as text, and run a turn —
/// unchanged from #11. Taken when `mcp.filepart_fallback` is set or MCP is disabled.
async fn handle_media_filepart(
    bot: Bot,
    msg: Message,
    state: Arc<AppState>,
    chat_id: i64,
    slot: Slot,
) -> ResponseResult<()> {
    match files::inbound_parts(&bot, &msg).await {
        Ok(Some(parts)) => enqueue_parts(&bot, &msg, &state, chat_id, slot, parts).await?,
        Ok(None) => {} // not a photo/document — nothing to do.
        Err(err) => {
            tracing::warn!(chat_id, slot = %slot.name, error = format!("{err:#}"), "inbound file failed");
            reply_inbound_file_failed(&bot, &msg).await?;
        }
    }
    Ok(())
}

/// The friendly reply shown when an inbound file can't be ingested — a download
/// failure, or the store rejecting/erroring on the bytes. Shared by both inbound
/// paths so they fail identically (the original #11 wording).
async fn reply_inbound_file_failed(bot: &Bot, msg: &Message) -> ResponseResult<()> {
    bot.send_message(
        msg.chat.id,
        "⚠️ I couldn't read that file — please try again.",
    )
    .await?;
    Ok(())
}

/// Resolve the sender's bound slot against the whitelist + runtime registry.
/// Returns `Some(slot)` to proceed; on an unauthorized sender it runs the
/// pairing flow ([`handle_unauthorized`]) and returns `None`; a DB error is
/// reported and returns `None`.
async fn resolve_or_reject(
    bot: &Bot,
    msg: &Message,
    state: &Arc<AppState>,
    chat_id: i64,
) -> ResponseResult<Option<Slot>> {
    let slots = state.slot_snapshot();
    match auth::resolve(&state.db, &slots, chat_id) {
        Ok(Some(slot)) => Ok(Some(slot)),
        Ok(None) => {
            handle_unauthorized(bot, msg, state, chat_id).await?;
            Ok(None)
        }
        Err(err) => {
            tracing::error!(chat_id, error = %err, "auth lookup failed");
            bot.send_message(msg.chat.id, ERROR_REPLY).await?;
            Ok(None)
        }
    }
}

/// Hand a resolved turn to the user's serial worker (#9): turns for one user run
/// one at a time, and a full queue is rejected here (with [`BUSY_REPLY`]) rather
/// than blocking the dispatcher. The reply is produced by the worker.
async fn enqueue_parts(
    bot: &Bot,
    msg: &Message,
    state: &Arc<AppState>,
    chat_id: i64,
    slot: Slot,
    parts: Vec<PartInput>,
) -> ResponseResult<()> {
    let job = TurnJob { slot, parts };
    match state.enqueue_turn(bot, chat_id, job) {
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
        let result = run_turn(&bot, &state, &job.slot, chat_id, job.parts)
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
    parts: Vec<PartInput>,
) -> anyhow::Result<()> {
    // Clone the client out of the registry under a short read lock; the guard is
    // dropped inside `client_for` before any await below.
    let client = state
        .client_for(&slot.name)
        .ok_or_else(|| anyhow::anyhow!("no opencode client for slot '{}'", slot.name))?;
    // The model selector resolved for this slot at connect (#74) — config
    // `[model]` or opencode's default.
    let model = state
        .model_for(&slot.name)
        .ok_or_else(|| anyhow::anyhow!("no resolved model for slot '{}'", slot.name))?;

    // Read routing from SQLite (sync, lock released before the await below).
    let stored = state.db.get_session(chat_id)?;
    let session_id = session::get_or_create(
        &client,
        stored.as_deref(),
        &model,
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
        &state.db,
        &slot.opencode_url,
        chat_id,
        &session_id,
        PromptModel::from(&model),
        parts,
        verbosity,
        state.context_limit_for(&slot.name),
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
