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
use std::sync::Arc;

use teloxide::prelude::*;
use teloxide::utils::command::BotCommands;
use tokio::sync::Mutex;

use crate::auth;
use crate::config::{Config, Slot};
use crate::opencode::client::OpencodeClient;
use crate::opencode::types::PromptModel;
use crate::session;
use crate::telegram::render::{self, TELEGRAM_LIMIT};

/// Shared dispatcher state: config, one opencode client per slot (keyed by slot
/// name), and the in-memory `chat_id → session_id` routing map.
///
/// The session map is a `tokio::sync::Mutex` — a plain guard around the map is
/// enough at this scale; the per-user `mpsc` turn-serialization queue is #9.
/// Persisting the routing map to SQLite is #3.
pub struct AppState {
    pub cfg: Config,
    pub clients: HashMap<String, OpencodeClient>,
    pub sessions: Mutex<HashMap<i64, String>>,
}

impl AppState {
    /// Build state from config + already-validated per-slot clients.
    pub fn new(cfg: Config, clients: HashMap<String, OpencodeClient>) -> Arc<Self> {
        Arc::new(Self {
            cfg,
            clients,
            sessions: Mutex::new(HashMap::new()),
        })
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
async fn handle_text(bot: Bot, msg: Message, state: Arc<AppState>) -> ResponseResult<()> {
    let Some(text) = msg.text() else {
        return Ok(()); // non-text (photo/doc/etc.) — inbound files are #8.
    };
    let chat_id = msg.chat.id.0;

    let Some(slot) = auth::resolve(&state.cfg, chat_id) else {
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
    let client = state
        .clients
        .get(&slot.name)
        .ok_or_else(|| anyhow::anyhow!("no opencode client for slot '{}'", slot.name))?;

    let stored = state.sessions.lock().await.get(&chat_id).cloned();
    let session_id = session::get_or_create(
        client,
        stored.as_deref(),
        &state.cfg.model,
        &state.cfg.permissions.ask,
    )
    .await?;
    state
        .sessions
        .lock()
        .await
        .insert(chat_id, session_id.clone());

    let reply = client
        .prompt(&session_id, PromptModel::from(&state.cfg.model), text)
        .await?;
    Ok(reply.text())
}
