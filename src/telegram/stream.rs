//! Streaming turn driver (§13, issue #8).
//!
//! Runs one turn against opencode with live Telegram feedback. It opens a
//! [`Subscription`](crate::opencode::events::Subscription) to the slot's
//! `/global/event` (#7), fires the **blocking** `POST /session/:id/message`, and
//! concurrently drives Telegram from the event stream:
//!
//! - text deltas accumulate in a [`LiveState`] and are flushed to one message via
//!   `editMessageText` on a **≤1/sec** ticker (only when the content changed) —
//!   Telegram's per-chat edit flood limit is why the throttle exists;
//! - a `typing` chat action is (re-)sent every ~4s for ambient liveness, off the
//!   edit budget;
//! - reasoning deltas are **not** streamed into the answer (they only keep the
//!   `typing` action alive, §13);
//! - when the blocking prompt returns, the message is finalized to the
//!   authoritative assistant text, rendered to Telegram MarkdownV2 and chunked
//!   with [`markdown::to_chunks`] if it exceeds [`TELEGRAM_LIMIT`] (#70), with
//!   tool failures always appended.
//!
//! The message is created lazily on the first flush that has something to show,
//! so a fast turn with no intermediate output just posts the final answer.

use std::collections::HashSet;
use std::time::Duration;

use anyhow::{Context, Result};
use teloxide::prelude::*;
use teloxide::types::{ChatAction, ChatId, Message, MessageId, ParseMode};
use teloxide::{ApiError, RequestError};

use crate::opencode::client::OpencodeClient;
use crate::opencode::events::{Event, PartKind, Subscription};
use crate::opencode::types::{PartInput, PromptModel};
use crate::permission;
use crate::persistence::Db;
use crate::telegram::markdown::{self, Chunk};
use crate::telegram::render::{LiveState, TELEGRAM_LIMIT, Verbosity};
use crate::telegram::retry;

/// Timing knobs for [`run_streaming_turn`], injectable so tests can run fast.
#[derive(Debug, Clone, Copy)]
pub struct StreamTiming {
    /// Minimum interval between live `editMessageText` flushes (≤1/sec live).
    pub flush_interval: Duration,
    /// Interval for re-sending the `typing` chat action (~4s live).
    pub typing_interval: Duration,
    /// `/global/event` reconnect delay handed to the [`Subscription`].
    pub retry: Duration,
}

impl Default for StreamTiming {
    fn default() -> Self {
        Self {
            flush_interval: Duration::from_secs(1),
            typing_interval: Duration::from_secs(4),
            retry: Duration::from_secs(3),
        }
    }
}

/// Drive one streaming turn to completion. `session_id` scopes which events on
/// the global stream belong to this turn; `slot_url` is the opencode base URL to
/// subscribe to. Returns once the assistant message has been finalized in
/// Telegram (or errors if the prompt itself failed).
#[allow(clippy::too_many_arguments)]
pub async fn run_streaming_turn(
    bot: &Bot,
    http: &reqwest::Client,
    client: &OpencodeClient,
    db: &Db,
    slot_url: &str,
    chat_id: i64,
    session_id: &str,
    model: PromptModel,
    parts: Vec<PartInput>,
    verbosity: Verbosity,
    context_limit: Option<u64>,
    timing: StreamTiming,
) -> Result<()> {
    // Subscribe BEFORE firing the prompt so no delta of this turn is missed.
    let mut subscription = Subscription::connect(http, slot_url, timing.retry)
        .context("subscribing to /global/event for the streaming turn")?;

    let mut state = LiveState::new(verbosity).with_context_limit(context_limit);
    // Part ids known to be reasoning — their deltas drive `typing`, not the answer.
    let mut reasoning_parts: HashSet<String> = HashSet::new();
    let mut sink = LiveSink::new(bot, chat_id);

    let prompt = client.prompt(session_id, model, parts);
    tokio::pin!(prompt);

    let mut flush_tick = tokio::time::interval(timing.flush_interval);
    let mut typing_tick = tokio::time::interval(timing.typing_interval);
    // `interval` yields an immediate first tick; consume both so they pace from
    // now. The initial `typing` is sent explicitly below for instant liveness.
    flush_tick.tick().await;
    typing_tick.tick().await;
    sink.send_typing().await;

    let mut stream_open = true;
    let reply = loop {
        tokio::select! {
            biased;
            // The blocking prompt completing ends the turn.
            res = &mut prompt => break res,
            _ = flush_tick.tick() => sink.flush(&state).await,
            _ = typing_tick.tick() => sink.send_typing().await,
            event = subscription.recv(), if stream_open => match event {
                None => stream_open = false, // terminal — stop polling this arm.
                // A permission gate for this turn (#13): post the approval buttons.
                // opencode holds the prompt blocked until the user taps one, which
                // the dispatcher answers via `reply_permission`.
                Some(Event::Permission(p)) if p.session_id == session_id => {
                    if let Err(err) = permission::prompt(bot, db, chat_id, &p).await {
                        tracing::warn!(error = %err, "posting permission prompt failed");
                    }
                }
                Some(ev) => apply_event(&mut state, &mut reasoning_parts, session_id, ev),
            },
        }
    };

    let reply = reply.context("streaming prompt failed")?;
    // The completed assistant message carries the authoritative token usage —
    // record it so the finalize footer can show context usage (#72).
    if let Some(tokens) = &reply.info.tokens {
        state.set_context_used(tokens.context_used());
    }
    sink.finalize(&state, &reply.text()).await
}

/// Route one event into the turn state, filtered to this turn's `session_id`.
fn apply_event(
    state: &mut LiveState,
    reasoning_parts: &mut HashSet<String>,
    session_id: &str,
    event: Event,
) {
    match event {
        Event::Delta(d) if d.session_id == session_id && d.field == "text" => {
            // Reasoning text is liveness-only (§13); only answer text streams.
            if !reasoning_parts.contains(&d.part_id) {
                state.push_text(&d.delta);
            }
        }
        Event::PartUpdated(p) if p.session_id == session_id => {
            if matches!(p.kind, PartKind::Reasoning) {
                reasoning_parts.insert(p.part_id.clone());
            }
            state.apply_part(&p.kind);
        }
        // session.status → covered by the unconditional typing keep-alive;
        // permission.asked → #13; everything else is not surfaced in B2.
        _ => {}
    }
}

/// Owns the single live Telegram message for a turn: created lazily, edited
/// in place, and dedup-guarded so we never send an identical edit (which
/// Telegram rejects as "message is not modified").
struct LiveSink<'a> {
    bot: &'a Bot,
    chat: ChatId,
    message_id: Option<MessageId>,
    last_sent: String,
}

impl<'a> LiveSink<'a> {
    fn new(bot: &'a Bot, chat_id: i64) -> Self {
        Self {
            bot,
            chat: ChatId(chat_id),
            message_id: None,
            last_sent: String::new(),
        }
    }

    /// `sendMessage` for a rendered [`Chunk`]: try the MarkdownV2 body, and on a
    /// Telegram parse rejection resend the raw-Markdown fallback as plain text so
    /// a message is never lost to a formatting slip (#70). Both paths retry
    /// flood/transient failures (#25).
    async fn send(&self, chunk: &Chunk) -> Result<Message, RequestError> {
        match self.send_formatted(&chunk.formatted).await {
            Err(err) if is_parse_error(&err) => {
                tracing::debug!("markdownv2 rejected by telegram; sending plain text");
                self.send_plain(&chunk.plain).await
            }
            other => other,
        }
    }

    /// `editMessageText` for a rendered [`Chunk`], with the same MarkdownV2 →
    /// plain-text fallback as [`send`](Self::send).
    async fn edit(&self, id: MessageId, chunk: &Chunk) -> Result<Message, RequestError> {
        match self.edit_formatted(id, &chunk.formatted).await {
            Err(err) if is_parse_error(&err) => {
                tracing::debug!("markdownv2 rejected by telegram; editing to plain text");
                self.edit_plain(id, &chunk.plain).await
            }
            other => other,
        }
    }

    /// `sendMessage` with `parse_mode=MarkdownV2`, retrying flood/transient
    /// failures (#25).
    async fn send_formatted(&self, text: &str) -> Result<Message, RequestError> {
        let bot = self.bot.clone();
        let chat = self.chat;
        let text = text.to_string();
        retry::with_retry("send_message", move || {
            let bot = bot.clone();
            let text = text.clone();
            async move {
                bot.send_message(chat, text)
                    .parse_mode(ParseMode::MarkdownV2)
                    .await
            }
        })
        .await
    }

    /// `sendMessage` with no parse mode — the plain-text fallback / system notes.
    async fn send_plain(&self, text: &str) -> Result<Message, RequestError> {
        let bot = self.bot.clone();
        let chat = self.chat;
        let text = text.to_string();
        retry::with_retry("send_message", move || {
            let bot = bot.clone();
            let text = text.clone();
            async move { bot.send_message(chat, text).await }
        })
        .await
    }

    /// `editMessageText` with `parse_mode=MarkdownV2`, retrying (#25).
    async fn edit_formatted(&self, id: MessageId, text: &str) -> Result<Message, RequestError> {
        let bot = self.bot.clone();
        let chat = self.chat;
        let text = text.to_string();
        retry::with_retry("edit_message_text", move || {
            let bot = bot.clone();
            let text = text.clone();
            async move {
                bot.edit_message_text(chat, id, text)
                    .parse_mode(ParseMode::MarkdownV2)
                    .await
            }
        })
        .await
    }

    /// `editMessageText` with no parse mode — the plain-text fallback / notes.
    async fn edit_plain(&self, id: MessageId, text: &str) -> Result<Message, RequestError> {
        let bot = self.bot.clone();
        let chat = self.chat;
        let text = text.to_string();
        retry::with_retry("edit_message_text", move || {
            let bot = bot.clone();
            let text = text.clone();
            async move { bot.edit_message_text(chat, id, text).await }
        })
        .await
    }

    /// Best-effort `typing` chat action (retried on flood/transient); a failure
    /// here is cosmetic.
    async fn send_typing(&self) {
        let bot = self.bot.clone();
        let chat = self.chat;
        let sent = retry::with_retry("send_chat_action", move || {
            let bot = bot.clone();
            async move { bot.send_chat_action(chat, ChatAction::Typing).await }
        })
        .await;
        if let Err(err) = sent {
            tracing::debug!(error = %err, "send_chat_action(typing) failed");
        }
    }

    /// Flush the current live view (throttled by the driver's ticker). Creates
    /// the message on first content, edits it thereafter, and skips no-op edits.
    /// A transient edit failure is logged, not fatal — the finalize will correct
    /// the message.
    async fn flush(&mut self, state: &LiveState) {
        if !state.has_content() {
            return;
        }
        // While streaming, only the first chunk is live-edited; the rest is sent
        // on finalize. Rendering to MarkdownV2 (#70) here keeps every edit both
        // within the per-message limit and valid for the current partial text.
        let content = state.render();
        let Some(chunk) = markdown::to_chunks(&content, TELEGRAM_LIMIT)
            .into_iter()
            .next()
        else {
            return;
        };
        // Telegram rejects a whitespace-only body ("text must be non-empty"), so
        // hold off until there is a visible character — e.g. a leading-whitespace
        // first token shouldn't create the message yet. Dedup on the rendered
        // form so an unchanged view is never re-sent.
        if chunk.plain.trim().is_empty() || chunk.formatted == self.last_sent {
            return;
        }
        match self.message_id {
            None => match self.send(&chunk).await {
                Ok(msg) => {
                    self.message_id = Some(msg.id);
                    self.last_sent = chunk.formatted;
                }
                Err(err) => tracing::warn!(error = %err, "creating live message failed"),
            },
            Some(id) => match self.edit(id, &chunk).await {
                Ok(_) => self.last_sent = chunk.formatted,
                Err(err) => tracing::debug!(error = %err, "live edit failed (will finalize)"),
            },
        }
    }

    /// Finalize the turn to the authoritative assistant text (failures appended),
    /// chunked across Telegram's limit: the live message becomes the first chunk
    /// (edited if it exists, else sent), and any overflow is sent as new messages.
    async fn finalize(&mut self, state: &LiveState, authoritative: &str) -> Result<()> {
        let final_text = state.finalize(authoritative);
        // A whitespace-only (or empty) reply has nothing Telegram will accept —
        // route it to the empty-reply note rather than a rejected send.
        let chunks = if final_text.trim().is_empty() {
            Vec::new()
        } else {
            markdown::to_chunks(&final_text, TELEGRAM_LIMIT)
        };

        let Some((first, rest)) = chunks.split_first() else {
            // Nothing to say — the model finished without a text answer (e.g. it
            // only reasoned or used a tool). Say so actionably rather than cryptic.
            let note = "⚠️ The model finished without a text answer (it may have only \
                        reasoned or used a tool). Try rephrasing, or /new to reset.";
            match self.message_id {
                Some(id) => {
                    let _ = self.edit_plain(id, note).await;
                }
                None => {
                    self.send_plain(note)
                        .await
                        .context("sending empty-reply note")?;
                }
            }
            return Ok(());
        };

        match self.message_id {
            Some(id) if first.formatted != self.last_sent => {
                self.edit(id, first)
                    .await
                    .context("finalizing live message")?;
            }
            Some(_) => {} // already showing the final first chunk.
            None => {
                self.send(first).await.context("sending final reply")?;
            }
        }
        for chunk in rest {
            self.send(chunk).await.context("sending overflow chunk")?;
        }
        Ok(())
    }
}

/// Whether a Telegram error is a MarkdownV2 parse rejection — the signal to fall
/// back to a plain-text send/edit (#70). Telegram reports this as
/// `Bad Request: can't parse entities: …`.
fn is_parse_error(err: &RequestError) -> bool {
    match err {
        RequestError::Api(ApiError::CantParseEntities(_)) => true,
        RequestError::Api(ApiError::Unknown(msg)) => {
            msg.to_ascii_lowercase().contains("can't parse entities")
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::is_parse_error;
    use teloxide::{ApiError, RequestError};

    #[test]
    fn parse_error_matches_the_typed_variant() {
        let err = RequestError::Api(ApiError::CantParseEntities("byte offset 12".into()));
        assert!(is_parse_error(&err));
    }

    #[test]
    fn parse_error_matches_an_unknown_cant_parse_message() {
        // Telegram sometimes surfaces this as a generic Unknown error string.
        let err = RequestError::Api(ApiError::Unknown(
            "Bad Request: can't parse entities: unexpected end of input".into(),
        ));
        assert!(is_parse_error(&err));
    }

    #[test]
    fn unrelated_api_errors_do_not_trigger_the_fallback() {
        // A "not modified" edit must NOT be treated as a parse failure.
        assert!(!is_parse_error(&RequestError::Api(
            ApiError::MessageNotModified
        )));
    }
}
