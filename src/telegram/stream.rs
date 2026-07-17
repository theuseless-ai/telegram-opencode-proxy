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
//!   `typing` action alive, §13) — classified order-independently by
//!   [`TextRouter`], since a delta can arrive before the part's kind is known;
//! - tool lifecycle updates feed the turn's activity log (#6), expanded in the
//!   live view and folded into a collapsed expandable blockquote on finalize
//!   at Verbose;
//! - when the blocking prompt returns, the message is finalized to the
//!   authoritative assistant text, rendered to Telegram MarkdownV2 and chunked
//!   with [`markdown::to_chunks`] if it exceeds [`TELEGRAM_LIMIT`] (#70), with
//!   tool failures always appended and the summary footer (#14) last.
//!
//! The message is created lazily on the first flush that has something to show,
//! so a fast turn with no intermediate output just posts the final answer.

use std::collections::{HashMap, HashSet};
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
    // Order-independent reasoning-vs-answer classification of text deltas (#6).
    let mut router = TextRouter::default();
    // Decides which of the instance-wide gates this turn owns, incl. those from
    // subagent sessions it spawned (#88).
    let mut scope = permission::TurnScope::new(session_id);
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
                Some(Event::Permission(p)) => {
                    // `/global/event` carries every session's gates, so resolve
                    // whose turn this one belongs to — ours directly, ours via a
                    // Task-spawned subagent (#88), or another chat's to answer.
                    if let Some(origin) = scope.resolve(client, &p.session_id).await
                        && let Err(err) = permission::prompt(bot, db, chat_id, &p, &origin).await
                    {
                        tracing::warn!(error = %err, "posting permission prompt failed");
                    }
                }
                Some(ev) => apply_event(&mut state, &mut router, session_id, ev),
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

/// Order-independent routing of `text`-field deltas into answer vs reasoning
/// (#6).
///
/// A `message.part.delta` carries only its `part_id` — never the part's type
/// (confirmed against the A0 wire captures in `fixtures/opencode/events/`) —
/// so whether a delta is visible answer text or reasoning is only knowable
/// from the part's `message.part.updated` frame, and nothing guarantees that
/// frame precedes the deltas. Deltas for a still-unclassified part are
/// buffered here and released (in arrival order) once a
/// `PartUpdated(kind = Text)` proves the part visible; a `Reasoning` marker
/// discards the buffer instead — so reasoning text can never leak into the
/// answer whichever frame lands first. A part that never gets classified stays
/// buffered, which is harmless: the finalize path re-renders from the
/// authoritative reply anyway.
#[derive(Debug, Default)]
struct TextRouter {
    /// Parts proven visible answer text (`PartUpdated(kind = Text)`).
    answer: HashSet<String>,
    /// Parts proven reasoning — their deltas are liveness-only (§13).
    reasoning: HashSet<String>,
    /// Buffered delta text for parts whose kind is not yet known.
    pending: HashMap<String, String>,
}

impl TextRouter {
    /// Route one `text`-field delta: `Some` text to append to the answer right
    /// now, or `None` while the delta is suppressed (reasoning) or buffered
    /// (part kind still unknown).
    fn route_delta(&mut self, part_id: &str, delta: &str) -> Option<String> {
        if self.answer.contains(part_id) {
            Some(delta.to_string())
        } else if self.reasoning.contains(part_id) {
            None
        } else {
            self.pending
                .entry(part_id.to_string())
                .or_default()
                .push_str(delta);
            None
        }
    }

    /// Record a part's kind from its `PartUpdated`. Returns buffered delta text
    /// to flush into the answer when the part turns out to be visible text.
    fn classify(&mut self, part_id: &str, kind: &PartKind) -> Option<String> {
        match kind {
            PartKind::Reasoning => {
                self.reasoning.insert(part_id.to_string());
                self.pending.remove(part_id);
                None
            }
            PartKind::Text => {
                self.answer.insert(part_id.to_string());
                self.pending.remove(part_id).filter(|s| !s.is_empty())
            }
            _ => None,
        }
    }
}

/// Route one event into the turn state, filtered to this turn's `session_id`.
fn apply_event(state: &mut LiveState, router: &mut TextRouter, session_id: &str, event: Event) {
    match event {
        Event::Delta(d) if d.session_id == session_id && d.field == "text" => {
            // Reasoning text is liveness-only (§13); only answer text streams —
            // via the router, so an early delta can't outrun its part's kind (#6).
            if let Some(text) = router.route_delta(&d.part_id, &d.delta) {
                state.push_text(&text);
            }
        }
        Event::PartUpdated(p) if p.session_id == session_id => {
            if let Some(buffered) = router.classify(&p.part_id, &p.kind) {
                state.push_text(&buffered);
            }
            state.apply_part(&p.kind);
        }
        // Recognised-but-unhandled frames: logged so a wire-shape drift (e.g. a
        // renamed part type no longer reaching `PartKind::Tool`) is diagnosable.
        Event::Other { kind } => tracing::trace!(kind = %kind, "unhandled opencode event"),
        // session.status → covered by the unconditional typing keep-alive;
        // permission.asked → #13; other sessions' frames are not ours to render.
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

    /// Finalize the turn to the authoritative assistant text (failures appended,
    /// footer last), chunked across Telegram's limit: the live message becomes
    /// the first chunk (edited if it exists, else sent), and any overflow is sent
    /// as new messages. At Verbose the collapsed activity log (#6) — already
    /// valid MarkdownV2, so it bypasses the body's Markdown conversion — is
    /// prepended to the first chunk.
    async fn finalize(&mut self, state: &LiveState, authoritative: &str) -> Result<()> {
        let message = state.finalize(authoritative);
        // A whitespace-only (or empty) reply has nothing Telegram will accept —
        // route it to the empty-reply note rather than a rejected send.
        let mut chunks = if message.body.trim().is_empty() {
            Vec::new()
        } else {
            // The collapsed log rides in the first chunk, so shrink the chunk
            // budget by its rendered size (+2 for the separating blank line) to
            // stay within Telegram's limit. The log is small by construction —
            // a windowed set of clipped lines (`render::LOG_WINDOW`).
            let limit = match &message.log {
                Some(log) => TELEGRAM_LIMIT
                    .saturating_sub(log.formatted.chars().count() + 2)
                    .max(1),
                None => TELEGRAM_LIMIT,
            };
            markdown::to_chunks(&message.body, limit)
        };
        if let Some(log) = &message.log
            && let Some(first) = chunks.first_mut()
        {
            // The blank line matters: it ends the expandable blockquote, so an
            // answer that itself starts with a `>` quote line is not swallowed
            // into the collapsed log.
            first.formatted = format!("{}\n\n{}", log.formatted, first.formatted);
            first.plain = format!("{}\n\n{}", log.plain, first.plain);
        }

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
    use super::{TextRouter, apply_event, is_parse_error};
    use crate::opencode::events::{Delta, Event, PartKind, PartUpdate};
    use crate::telegram::render::{LiveState, Verbosity};
    use teloxide::{ApiError, RequestError};

    const SESSION: &str = "ses_1";

    fn delta(part_id: &str, text: &str) -> Event {
        Event::Delta(Delta {
            session_id: SESSION.to_string(),
            message_id: "msg_1".to_string(),
            part_id: part_id.to_string(),
            field: "text".to_string(),
            delta: text.to_string(),
        })
    }

    fn part(part_id: &str, kind: PartKind) -> Event {
        Event::PartUpdated(PartUpdate {
            session_id: SESSION.to_string(),
            message_id: "msg_1".to_string(),
            part_id: part_id.to_string(),
            kind,
        })
    }

    // --- reasoning/text delta race (#6) ----------------------------------------

    #[test]
    fn reasoning_marker_before_deltas_suppresses_them() {
        // The common ordering (per the A0 fixtures): marker first, then deltas.
        let mut state = LiveState::new(Verbosity::Normal);
        let mut router = TextRouter::default();
        apply_event(
            &mut state,
            &mut router,
            SESSION,
            part("p1", PartKind::Reasoning),
        );
        apply_event(&mut state, &mut router, SESSION, delta("p1", "thinking…"));
        assert_eq!(state.render(), "");
    }

    #[test]
    fn reasoning_deltas_arriving_before_their_marker_do_not_leak() {
        // The race (#6): a `text`-field delta lands before the frame that says
        // its part is reasoning. It must be buffered, then discarded — never
        // shown as answer text.
        let mut state = LiveState::new(Verbosity::Normal);
        let mut router = TextRouter::default();
        apply_event(&mut state, &mut router, SESSION, delta("p1", "secret "));
        apply_event(&mut state, &mut router, SESSION, delta("p1", "thoughts"));
        assert_eq!(state.render(), "", "unclassified deltas stay buffered");
        apply_event(
            &mut state,
            &mut router,
            SESSION,
            part("p1", PartKind::Reasoning),
        );
        apply_event(&mut state, &mut router, SESSION, delta("p1", " more"));
        assert_eq!(state.render(), "", "reasoning never reaches the answer");
    }

    #[test]
    fn text_deltas_arriving_before_their_marker_flush_in_order() {
        let mut state = LiveState::new(Verbosity::Normal);
        let mut router = TextRouter::default();
        apply_event(&mut state, &mut router, SESSION, delta("p1", "Hello"));
        apply_event(&mut state, &mut router, SESSION, delta("p1", ", world"));
        assert_eq!(state.render(), "", "not shown until the part is classified");
        apply_event(&mut state, &mut router, SESSION, part("p1", PartKind::Text));
        assert_eq!(state.render(), "Hello, world");
        // Later deltas for the now-classified part append directly.
        apply_event(&mut state, &mut router, SESSION, delta("p1", "!"));
        assert_eq!(state.render(), "Hello, world!");
    }

    #[test]
    fn interleaved_reasoning_and_text_parts_route_independently() {
        let mut state = LiveState::new(Verbosity::Normal);
        let mut router = TextRouter::default();
        apply_event(&mut state, &mut router, SESSION, delta("think", "hmm"));
        apply_event(&mut state, &mut router, SESSION, delta("say", "Answer"));
        apply_event(
            &mut state,
            &mut router,
            SESSION,
            part("say", PartKind::Text),
        );
        apply_event(
            &mut state,
            &mut router,
            SESSION,
            part("think", PartKind::Reasoning),
        );
        assert_eq!(state.render(), "Answer");
    }

    #[test]
    fn other_sessions_events_are_ignored() {
        let mut state = LiveState::new(Verbosity::Normal);
        let mut router = TextRouter::default();
        apply_event(
            &mut state,
            &mut router,
            "ses_other",
            part("p1", PartKind::Text),
        );
        apply_event(
            &mut state,
            &mut router,
            "ses_other",
            delta("p1", "not ours"),
        );
        assert_eq!(state.render(), "");
    }

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
