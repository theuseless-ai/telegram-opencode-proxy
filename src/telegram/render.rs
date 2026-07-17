//! opencode output → Telegram: 4096-char chunking, ≤1/sec stream-edit throttle,
//! `typing` liveness, a persistent tool **activity log** (#6), and a completion
//! summary footer (#14). See `docs/design/architecture.md` §13. Issues
//! #6/#8/#10/#14.
//!
//! [`LiveState`] is
//! the B2 streaming render machine (#8): a pure state accumulator the streaming
//! turn driver (`telegram::stream`) feeds opencode events into, asking it what a
//! single Telegram message should currently show. It does **no** I/O — the driver
//! owns the throttle ticker and the `editMessageText` calls — so the layout and
//! coalescing logic are unit-testable without a `Bot`.
//!
//! Tool activity accumulates in an ordered log (#6) that is **expanded** while
//! the turn runs (under a `🔧 Working…` header, above the streaming answer) and
//! **folds** into a collapsed Telegram expandable blockquote on finalize at
//! Verbose — Quiet shows no activity at all, Normal shows the live log but drops
//! it from the final message, keeping just the summary footer.

use crate::opencode::events::{PartKind, ToolStatus};
use crate::telegram::markdown;

/// Telegram's hard per-message limit (characters).
pub const TELEGRAM_LIMIT: usize = 4096;

/// Per-user output verbosity (`/quiet` · `/verbose`, §13, #10). Persisted per
/// chat; the streaming renderer reads it to decide how much to surface. `Normal`
/// is the default. Concrete effects (#6/#14):
///
/// | level   | live (during turn)                   | final message              |
/// |---------|--------------------------------------|----------------------------|
/// | Quiet   | answer stream only (typing liveness) | answer only — no footer    |
/// | Normal  | activity log, bare tool names        | answer + summary footer    |
/// | Verbose | log with `title`s + `💬` narration   | collapsed log + answer + footer |
///
/// Tool **failures** are always shown at every level, outside any collapse.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Verbosity {
    /// Answer (and failures) only — no activity log, no footer.
    Quiet,
    /// Answer stream + live activity log (bare tool names); footer on finalize.
    #[default]
    Normal,
    /// Like `Normal`, plus tool `title`s and `💬` narration in the log, and the
    /// log folded into a collapsed expandable blockquote on finalize (#6).
    Verbose,
}

impl Verbosity {
    /// The persisted string form.
    pub fn as_str(self) -> &'static str {
        match self {
            Verbosity::Quiet => "quiet",
            Verbosity::Normal => "normal",
            Verbosity::Verbose => "verbose",
        }
    }

    /// Parse the persisted form; anything unrecognised falls back to `Normal`.
    pub fn from_stored(s: &str) -> Self {
        match s {
            "quiet" => Verbosity::Quiet,
            "verbose" => Verbosity::Verbose,
            _ => Verbosity::Normal,
        }
    }

    /// Whether the live activity log should be shown at this level.
    fn shows_tool_line(self) -> bool {
        !matches!(self, Verbosity::Quiet)
    }
}

/// Activity-log rolling window (#6): at most this many trailing entries are
/// rendered (live and in the final collapsed log), with an `… +N earlier`
/// marker standing in for anything older — a long turn must never let the log
/// crowd the answer out of Telegram's per-message limit.
const LOG_WINDOW: usize = 8;

/// Max chars for a tool `title` / narration echo within one log line. opencode
/// titles can be whole command lines; clipping keeps every log line — and thus
/// the whole windowed log — boundedly small (#6).
const LOG_LABEL_MAX: usize = 64;

/// One entry in the turn's ordered activity log (#6).
#[derive(Debug, Clone, PartialEq, Eq)]
enum LogEntry {
    /// A tool call, updated **in place** (matched by `call_id`) as its status
    /// advances pending → running → completed/error, so the log stays one line
    /// per call. `title` is opencode's `state.title` arg-summary (#14), kept
    /// current across updates (the pending state usually lacks it).
    Tool {
        name: String,
        call_id: String,
        title: Option<String>,
        status: LogStatus,
    },
    /// A `💬` echo of intermediate narration — answer text that streamed before
    /// another tool started (Verbose only). The text also stays in the answer;
    /// this is just its timeline marker (#6).
    Narration(String),
}

/// A log entry's display status: `⚙️`/`🧵` while in flight, `✓`/`✗` terminal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LogStatus {
    Running,
    Done,
    Failed,
}

/// The streaming render state for one turn (#6: activity log + answer, failures
/// always shown).
///
/// The driver pushes text deltas and tool-part updates in; [`render`](Self::render)
/// returns the text the live Telegram message should show **right now**:
///
/// - the expanded activity log under a `🔧 Working…` header (unless Quiet);
/// - below it, the streaming answer text;
/// - tool failures appended and kept at every stage (`✗ bash: …`).
///
/// The driver edits the message on a ≤1/sec ticker and only when [`render`] has
/// changed, so this type carries no timing — just the content.
#[derive(Debug, Default, Clone)]
pub struct LiveState {
    /// Accumulated visible answer text (concatenated `text` deltas).
    answer: String,
    /// The ordered activity log (#6): tool lines + `💬` narration markers.
    log: Vec<LogEntry>,
    /// Byte offset into `answer` already echoed into the log as `💬` narration
    /// (Verbose): the live view shows only the tail beyond it, so narration is
    /// not shown twice. Always a previous `answer.len()`, hence a char boundary.
    narration_mark: usize,
    /// Tool failures, shown at every stage and preserved into the final render.
    failures: Vec<String>,
    /// The user's output verbosity (#10) — gates the activity log.
    verbosity: Verbosity,
    /// Tool `call_id`s that have reached a terminal state, so the summary counts
    /// (#14) tally each tool once across its pending→running→terminal updates.
    counted_calls: std::collections::HashSet<String>,
    /// Completion tallies for the summary footer (#14): total tools run, of which
    /// `task` sub-agents, and `edit`/`write` file edits.
    tools: usize,
    subagents: usize,
    files_edited: usize,
    /// Context tokens consumed this turn (from the assistant message's usage),
    /// and the model's context-window size — together they render the context-%
    /// footer segment (#72). Both `None` until known.
    context_used: Option<u64>,
    context_limit: Option<u64>,
}

impl LiveState {
    /// A render state at the given output verbosity (#10).
    pub fn new(verbosity: Verbosity) -> Self {
        Self {
            verbosity,
            ..Self::default()
        }
    }

    /// Set the model's context-window size (#72), consumed builder-style. `None`
    /// leaves the footer to fall back to a raw token count instead of a %.
    pub fn with_context_limit(mut self, limit: Option<u64>) -> Self {
        self.context_limit = limit;
        self
    }

    /// Record the turn's context-token usage for the footer (#72). Called with
    /// the authoritative count from the completed assistant message.
    pub fn set_context_used(&mut self, used: u64) {
        self.context_used = Some(used);
    }

    /// Append a streamed text chunk to the answer buffer, shown below the
    /// activity log in [`render`] (#6).
    pub fn push_text(&mut self, delta: &str) {
        self.answer.push_str(delta);
    }

    /// Apply a `message.part.updated` tool lifecycle: upsert the tool's activity
    /// log line (#6) and record failures (kept visible at every verbosity, §13).
    /// Non-tool parts are ignored here — text arrives via [`push_text`].
    pub fn apply_part(&mut self, kind: &PartKind) {
        if let PartKind::Tool {
            name,
            call_id,
            status,
            title,
        } = kind
        {
            match status {
                ToolStatus::Pending | ToolStatus::Running => {
                    self.upsert_tool(name, call_id, title, LogStatus::Running);
                }
                ToolStatus::Completed => {
                    self.upsert_tool(name, call_id, title, LogStatus::Done);
                    self.count_tool(name, call_id);
                }
                ToolStatus::Error => {
                    self.upsert_tool(name, call_id, title, LogStatus::Failed);
                    self.count_tool(name, call_id);
                    let line = format!("✗ {name}: failed");
                    if !self.failures.contains(&line) {
                        self.failures.push(line);
                    }
                }
                ToolStatus::Other(_) => {}
            }
        }
    }

    /// Create or update the log line for one tool call (#6). Matched by
    /// `call_id` so a call's pending→running→terminal updates stay one line;
    /// `title` is refreshed whenever an update carries one (the pending state
    /// usually doesn't yet). A terminal status is never downgraded back to
    /// `Running` — across an SSE reconnect opencode re-emits the in-flight
    /// message's parts, and a replayed frame must not "restart" a done tool.
    fn upsert_tool(
        &mut self,
        name: &str,
        call_id: &str,
        title: &Option<String>,
        status: LogStatus,
    ) {
        let existing = self.log.iter_mut().find_map(|entry| match entry {
            LogEntry::Tool {
                call_id: id,
                title,
                status,
                ..
            } if id == call_id => Some((title, status)),
            _ => None,
        });
        match existing {
            Some((t, s)) => {
                if title.is_some() {
                    *t = title.clone();
                }
                if *s == LogStatus::Running || status != LogStatus::Running {
                    *s = status;
                }
            }
            None => {
                // A new tool starting closes any narration streamed before it —
                // echo that text into the timeline first so the log reads in
                // true chronological order (#6, Verbose only).
                self.echo_narration();
                self.log.push(LogEntry::Tool {
                    name: name.to_string(),
                    call_id: call_id.to_string(),
                    title: title.clone(),
                    status,
                });
            }
        }
    }

    /// Fold answer text streamed since the last echo into a `💬` narration log
    /// entry (#6) — Verbose only; Quiet/Normal never advance the mark, so their
    /// live answer keeps showing everything. The text is **not** removed from
    /// the answer itself: the final message always renders the authoritative
    /// reply, narration included — the log entry is just its timeline marker.
    fn echo_narration(&mut self) {
        if !matches!(self.verbosity, Verbosity::Verbose) {
            return;
        }
        let pending = self.answer[self.narration_mark..].trim();
        if !pending.is_empty() {
            self.log
                .push(LogEntry::Narration(clip(pending, LOG_LABEL_MAX)));
        }
        self.narration_mark = self.answer.len();
    }

    /// Tally a tool that reached a terminal state, once per `call_id`, into the
    /// summary-footer counts (#14): every terminal tool bumps `tools`; a `task`
    /// child bumps `subagents`; an `edit`/`write` bumps `files_edited`.
    fn count_tool(&mut self, name: &str, call_id: &str) {
        if !self.counted_calls.insert(call_id.to_string()) {
            return; // already tallied (an earlier terminal update for this call).
        }
        self.tools += 1;
        if name == "task" {
            self.subagents += 1;
        } else if matches!(name, "edit" | "write") {
            self.files_edited += 1;
        }
    }

    /// The one-line completion summary footer (#14, #72), or `None` when it
    /// should be omitted — in Quiet mode, or when there is nothing to report
    /// (no tool ran and no context usage is known). Shown above the answer on
    /// finalize (§13); zero categories are dropped, e.g.
    /// `✓ 3 tools · edited 1 file · 🧠 42%`.
    ///
    /// The `✓` marks the **tool** completion summary; a context-only footer (a
    /// plain text answer with no tools) carries no `✓`, just `🧠 42%`.
    fn summary_footer(&self) -> Option<String> {
        if matches!(self.verbosity, Verbosity::Quiet) {
            return None;
        }
        let mut segments = Vec::new();
        if self.tools > 0 {
            let mut tools = vec![plural(self.tools, "tool", "tools")];
            if self.subagents > 0 {
                tools.push(plural(self.subagents, "subagent", "subagents"));
            }
            if self.files_edited > 0 {
                tools.push(format!(
                    "edited {}",
                    plural(self.files_edited, "file", "files")
                ));
            }
            segments.push(format!("✓ {}", tools.join(" · ")));
        }
        // Context usage shows on every turn it is known — including a plain text
        // answer with no tools — so the user can watch it climb toward compaction.
        if let Some(segment) = self.context_segment() {
            segments.push(segment);
        }
        if segments.is_empty() {
            return None;
        }
        Some(segments.join(" · "))
    }

    /// The context-usage footer segment (#72): `🧠 42%` when the context-window
    /// size is known, otherwise a raw count like `🧠 12.3k ctx`. `None` until a
    /// usage figure has been recorded.
    fn context_segment(&self) -> Option<String> {
        let used = self.context_used?;
        match self.context_limit {
            Some(limit) if limit > 0 => {
                let pct = ((used as f64 / limit as f64) * 100.0).round() as u64;
                Some(format!("🧠 {pct}%"))
            }
            _ => Some(format!("🧠 {} ctx", human_tokens(used))),
        }
    }

    /// Format one activity-log entry (#6). Tools render as
    /// `<glyph> <name>` — with opencode's `title` arg-summary appended after
    /// `·` at Verbose, and always for a `task` sub-agent (whose title carries
    /// the sub-agent name/description). The glyph tracks the lifecycle: `⚙️`
    /// in flight (`🧵` for a sub-agent) flipping to `✓`/`✗` on completion.
    /// Narration entries render as `💬 <echo>`.
    fn log_line(&self, entry: &LogEntry) -> String {
        match entry {
            LogEntry::Narration(text) => format!("💬 {text}"),
            LogEntry::Tool {
                name,
                title,
                status,
                ..
            } => {
                let glyph = match status {
                    LogStatus::Running if name == "task" => "🧵",
                    LogStatus::Running => "⚙️",
                    LogStatus::Done => "✓",
                    LogStatus::Failed => "✗",
                };
                let titled = title
                    .as_deref()
                    .filter(|s| !s.is_empty())
                    .filter(|_| name == "task" || matches!(self.verbosity, Verbosity::Verbose));
                match titled {
                    Some(t) => format!("{glyph} {name} · {}", clip(t, LOG_LABEL_MAX)),
                    None => format!("{glyph} {name}"),
                }
            }
        }
    }

    /// The rendered log window (#6): the trailing [`LOG_WINDOW`] entries as
    /// lines, preceded by an `… +N earlier` marker when older entries had to be
    /// dropped to stay within the length cap. Empty when the log is.
    fn log_lines(&self) -> Vec<String> {
        let hidden = self.log.len().saturating_sub(LOG_WINDOW);
        let mut lines = Vec::with_capacity(self.log.len() - hidden + 1);
        if hidden > 0 {
            lines.push(format!("… +{hidden} earlier"));
        }
        for entry in &self.log[hidden..] {
            lines.push(self.log_line(entry));
        }
        lines
    }

    /// Whether the activity log is visible at the current verbosity and has
    /// anything to show (hidden entirely in Quiet, #10).
    fn log_visible(&self) -> bool {
        self.verbosity.shows_tool_line() && !self.log.is_empty()
    }

    /// The live answer tail: everything streamed since the last `💬` narration
    /// echo (#6). At Quiet/Normal the mark never advances, so this is the whole
    /// answer.
    fn answer_tail(&self) -> &str {
        &self.answer[self.narration_mark..]
    }

    /// Whether any visible content exists yet (answer, a shown log, or a
    /// failure) — the driver uses this to defer creating the Telegram message
    /// until there is something to show.
    pub fn has_content(&self) -> bool {
        !self.answer_tail().is_empty() || self.log_visible() || !self.failures.is_empty()
    }

    /// The full text the live message should show now — the expanded activity
    /// log under a `🔧 Working…` header (unless Quiet), the streaming answer
    /// below it, plus any failure lines. May exceed [`TELEGRAM_LIMIT`]; the
    /// driver renders it to MarkdownV2 and clamps to the first chunk while
    /// streaming (`telegram::markdown::to_chunks`, #70), chunking the rest on
    /// finalize.
    pub fn render(&self) -> String {
        let mut out = String::new();
        if self.log_visible() {
            out.push_str("🔧 Working…\n");
            out.push_str(&self.log_lines().join("\n"));
        }
        let tail = self.answer_tail();
        if !tail.trim().is_empty() {
            if !out.is_empty() {
                out.push_str("\n\n");
            }
            out.push_str(tail);
        }
        for failure in &self.failures {
            if !out.is_empty() {
                out.push('\n');
            }
            out.push_str(failure);
        }
        out
    }

    /// The finalized message content (#6): the Markdown `body` — authoritative
    /// answer (no tool decoration), failures appended so a blocked command is
    /// never silently dropped, then the summary footer (#14) **last** — plus, at
    /// Verbose, the activity log folded into a collapsed expandable blockquote
    /// for the driver to prepend to the first chunk.
    pub fn finalize(&self, authoritative: &str) -> FinalMessage {
        let mut body = String::from(authoritative);
        for failure in &self.failures {
            if !body.is_empty() {
                body.push('\n');
            }
            body.push_str(failure);
        }
        if let Some(footer) = self.summary_footer() {
            if !body.is_empty() {
                body.push('\n');
            }
            body.push_str(&footer);
        }
        let log = (matches!(self.verbosity, Verbosity::Verbose) && !self.log.is_empty())
            .then(|| self.collapsed_log());
        FinalMessage { log, body }
    }

    /// The folded activity log for the final message (#6): a `🔧 N tools`
    /// header line plus the same rolling window the live view shows, rendered
    /// both as a MarkdownV2 expandable blockquote (each line defensively
    /// escaped — tool names and opencode titles are outside our control) and as
    /// bare lines for the plain-text fallback path (#70).
    fn collapsed_log(&self) -> CollapsedLog {
        let tools = self
            .log
            .iter()
            .filter(|e| matches!(e, LogEntry::Tool { .. }))
            .count();
        let mut lines = vec![format!("🔧 {}", plural(tools, "tool", "tools"))];
        lines.extend(self.log_lines());
        let escaped: Vec<String> = lines.iter().map(|l| markdown::escape(l)).collect();
        CollapsedLog {
            formatted: markdown::expandable_quote(&escaped),
            plain: lines.join("\n"),
        }
    }
}

/// A finalized turn (#6), handed from [`LiveState::finalize`] to the driver:
/// the Markdown `body` to convert and chunk (`telegram::markdown::to_chunks`),
/// plus — at Verbose — the collapsed activity log to prepend to the first
/// chunk, already rendered to MarkdownV2 (the body pipeline would escape its
/// `**>` markers as literal text, so it must bypass conversion).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FinalMessage {
    /// The collapsed activity log, or `None` (Quiet/Normal, or an empty log).
    pub log: Option<CollapsedLog>,
    /// Markdown body: authoritative answer, failures, summary footer last.
    pub body: String,
}

/// The folded activity log (#6): `formatted` is a ready MarkdownV2 expandable
/// blockquote; `plain` is the same lines bare, for the plain-text fallback the
/// driver sends when Telegram rejects the formatted message (#70).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CollapsedLog {
    pub formatted: String,
    pub plain: String,
}

/// The first line of `text`, truncated to `max` chars — with a trailing `…`
/// whenever anything (further chars or further lines) was cut. Keeps every
/// activity-log line boundedly small (#6).
fn clip(text: &str, max: usize) -> String {
    let text = text.trim();
    let first = text.lines().next().unwrap_or("");
    let clipped: String = first.chars().take(max).collect();
    if clipped.len() < text.len() {
        format!("{clipped}…")
    } else {
        clipped
    }
}

/// `"{n} {singular}"` or `"{n} {plural}"` by count — e.g. `plural(1, "file",
/// "files") == "1 file"`, `plural(2, …) == "2 files"`.
fn plural(n: usize, singular: &str, plural: &str) -> String {
    format!("{n} {}", if n == 1 { singular } else { plural })
}

/// A compact token count: `950 → "950"`, `12345 → "12.3k"`, `1_200_000 → "1.2M"`.
/// Used by the context-usage footer when no context-window % can be shown (#72).
fn human_tokens(n: u64) -> String {
    if n < 1_000 {
        n.to_string()
    } else if n < 1_000_000 {
        format!("{:.1}k", n as f64 / 1_000.0)
    } else {
        format!("{:.1}M", n as f64 / 1_000_000.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tool(name: &str, status: ToolStatus) -> PartKind {
        tool_id(name, "call_x", status)
    }

    /// Like [`tool`] but with an explicit `call_id`, so the summary counts (#14)
    /// can be exercised with several distinct tool calls.
    fn tool_id(name: &str, call_id: &str, status: ToolStatus) -> PartKind {
        PartKind::Tool {
            name: name.to_string(),
            call_id: call_id.to_string(),
            status,
            title: None,
        }
    }

    /// A tool part carrying opencode's `state.title` summary (#14).
    fn tool_titled(name: &str, status: ToolStatus, title: &str) -> PartKind {
        tool_id_titled(name, "call_x", status, title)
    }

    /// Like [`tool_titled`] with an explicit `call_id`, for multi-tool logs (#6).
    fn tool_id_titled(name: &str, call_id: &str, status: ToolStatus, title: &str) -> PartKind {
        PartKind::Tool {
            name: name.to_string(),
            call_id: call_id.to_string(),
            status,
            title: Some(title.to_string()),
        }
    }

    #[test]
    fn live_state_shows_the_activity_log_before_any_answer() {
        let mut s = LiveState::new(Verbosity::Normal);
        assert!(!s.has_content());
        s.apply_part(&tool("bash", ToolStatus::Running));
        assert!(s.has_content());
        assert_eq!(s.render(), "🔧 Working…\n⚙️ bash");
    }

    #[test]
    fn quiet_hides_the_activity_log_but_not_failures_or_answer() {
        let mut s = LiveState::new(Verbosity::Quiet);
        // A running tool produces no visible content in Quiet mode.
        s.apply_part(&tool("bash", ToolStatus::Running));
        assert!(!s.has_content(), "quiet mode hides the activity log");
        assert_eq!(s.render(), "");
        // Failures are still always shown.
        s.apply_part(&tool("bash", ToolStatus::Error));
        assert_eq!(s.render(), "✗ bash: failed");
        // And the answer streams as usual.
        s.push_text("done");
        assert_eq!(s.render(), "done\n✗ bash: failed");
    }

    #[test]
    fn verbosity_round_trips_through_its_stored_form() {
        for v in [Verbosity::Quiet, Verbosity::Normal, Verbosity::Verbose] {
            assert_eq!(Verbosity::from_stored(v.as_str()), v);
        }
        assert_eq!(Verbosity::from_stored("bogus"), Verbosity::Normal);
        assert_eq!(Verbosity::default(), Verbosity::Normal);
    }

    #[test]
    fn live_answer_streams_below_the_persistent_log() {
        let mut s = LiveState::new(Verbosity::Normal);
        s.apply_part(&tool("bash", ToolStatus::Running));
        s.push_text("The output ");
        s.push_text("is hi.");
        // The log persists above the streaming answer (#6) — it no longer
        // vanishes once answer text exists.
        assert_eq!(s.render(), "🔧 Working…\n⚙️ bash\n\nThe output is hi.");
    }

    #[test]
    fn tool_status_glyph_flips_on_completion() {
        let mut s = LiveState::new(Verbosity::Normal);
        s.apply_part(&tool("bash", ToolStatus::Pending));
        assert_eq!(s.render(), "🔧 Working…\n⚙️ bash");
        s.apply_part(&tool("bash", ToolStatus::Running));
        assert_eq!(s.render(), "🔧 Working…\n⚙️ bash", "one line per call");
        s.apply_part(&tool("bash", ToolStatus::Completed));
        // The line flips ⚙️ → ✓ in place and stays in the log.
        assert!(s.has_content());
        assert_eq!(s.render(), "🔧 Working…\n✓ bash");
    }

    #[test]
    fn tool_status_glyph_flips_to_a_cross_on_error() {
        let mut s = LiveState::new(Verbosity::Normal);
        s.apply_part(&tool("bash", ToolStatus::Running));
        s.apply_part(&tool("bash", ToolStatus::Error));
        // The log line flips to ✗ AND the failure line renders outside the log.
        assert_eq!(s.render(), "🔧 Working…\n✗ bash\n✗ bash: failed");
    }

    #[test]
    fn terminal_status_is_not_downgraded_by_a_replayed_running_frame() {
        // Across an SSE reconnect opencode re-emits the in-flight message's
        // parts; a stale-ordered running frame must not "restart" a done tool.
        let mut s = LiveState::new(Verbosity::Normal);
        s.apply_part(&tool("bash", ToolStatus::Completed));
        s.apply_part(&tool("bash", ToolStatus::Running));
        assert_eq!(s.render(), "🔧 Working…\n✓ bash");
    }

    #[test]
    fn live_state_failures_always_shown() {
        let mut s = LiveState::new(Verbosity::Quiet);
        s.apply_part(&tool("bash", ToolStatus::Error));
        // A failure surfaces even with no answer text (and no log at Quiet).
        assert_eq!(s.render(), "✗ bash: failed");
        s.push_text("The command was blocked.");
        assert_eq!(s.render(), "The command was blocked.\n✗ bash: failed");
        // Dedup: the same failure is not repeated.
        s.apply_part(&tool("bash", ToolStatus::Error));
        assert_eq!(s.render().matches("✗ bash: failed").count(), 1);
    }

    #[test]
    fn live_state_finalize_appends_failures_then_the_footer() {
        let mut s = LiveState::new(Verbosity::Normal);
        s.push_text("partial…"); // streamed buffer is ignored by finalize
        s.apply_part(&tool("bash", ToolStatus::Error));
        // The errored tool both counts toward the footer and lists its failure;
        // the footer comes last (#6), and Normal folds no log into the final.
        let m = s.finalize("The full answer.");
        assert_eq!(m.body, "The full answer.\n✗ bash: failed\n✓ 1 tool");
        assert_eq!(m.log, None);
    }

    // --- summary footer (#14) --------------------------------------------------

    #[test]
    fn footer_counts_tools_subagents_and_file_edits() {
        let mut s = LiveState::new(Verbosity::Normal);
        for (name, id) in [
            ("bash", "c1"),
            ("read", "c2"),
            ("grep", "c3"),
            ("task", "c4"),
            ("edit", "c5"),
            ("write", "c6"),
        ] {
            s.apply_part(&tool_id(name, id, ToolStatus::Completed));
        }
        // 6 tools total; 1 of them a subagent (task); 2 file edits (edit + write).
        assert_eq!(
            s.finalize("Done.").body,
            "Done.\n✓ 6 tools · 1 subagent · edited 2 files"
        );
    }

    #[test]
    fn footer_omits_zero_categories_and_singularizes() {
        let mut s = LiveState::new(Verbosity::Normal);
        s.apply_part(&tool_id("edit", "c1", ToolStatus::Completed));
        // One edit tool: "1 tool" (singular) + "edited 1 file"; no subagent clause.
        assert_eq!(s.finalize("ok").body, "ok\n✓ 1 tool · edited 1 file");
    }

    #[test]
    fn footer_hidden_in_quiet_and_absent_without_tools() {
        // Quiet: no footer even though a tool ran.
        let mut q = LiveState::new(Verbosity::Quiet);
        q.apply_part(&tool_id("bash", "c1", ToolStatus::Completed));
        assert_eq!(q.finalize("ans").body, "ans");
        // Normal but no tools: a plain text answer gets no footer.
        let plain = LiveState::new(Verbosity::Normal);
        assert_eq!(plain.finalize("just text").body, "just text");
    }

    #[test]
    fn footer_tallies_each_call_once_across_updates() {
        let mut s = LiveState::new(Verbosity::Normal);
        // The same call_id transitions pending → running → completed: counted once.
        s.apply_part(&tool_id("bash", "c1", ToolStatus::Pending));
        s.apply_part(&tool_id("bash", "c1", ToolStatus::Running));
        s.apply_part(&tool_id("bash", "c1", ToolStatus::Completed));
        s.apply_part(&tool_id("bash", "c1", ToolStatus::Completed)); // duplicate terminal
        assert_eq!(s.finalize("done").body, "done\n✓ 1 tool");
    }

    // --- sub-agent tag + verbose tool args (#6/#14) ------------------------------

    #[test]
    fn task_tool_renders_as_a_subagent_line_with_its_title() {
        let mut s = LiveState::new(Verbosity::Normal);
        s.apply_part(&tool_titled("task", ToolStatus::Running, "explore"));
        // A running `task` child shows the 🧵 sub-agent line with its title —
        // at every non-quiet verbosity (the title carries the sub-agent name).
        assert_eq!(s.render(), "🔧 Working…\n🧵 task · explore");
    }

    #[test]
    fn task_tool_without_title_falls_back_to_a_bare_line() {
        let mut s = LiveState::new(Verbosity::Normal);
        s.apply_part(&tool("task", ToolStatus::Running));
        assert_eq!(s.render(), "🔧 Working…\n🧵 task");
    }

    #[test]
    fn verbose_shows_the_tool_title_normal_shows_the_bare_name() {
        // Verbose surfaces `name · title` (#6); Normal stays bare.
        let mut v = LiveState::new(Verbosity::Verbose);
        v.apply_part(&tool_titled("bash", ToolStatus::Running, "git status"));
        assert_eq!(v.render(), "🔧 Working…\n⚙️ bash · git status");

        let mut n = LiveState::new(Verbosity::Normal);
        n.apply_part(&tool_titled("bash", ToolStatus::Running, "git status"));
        assert_eq!(n.render(), "🔧 Working…\n⚙️ bash");
    }

    #[test]
    fn late_title_updates_the_existing_log_line() {
        // The pending state usually carries no title yet; the running update does.
        let mut s = LiveState::new(Verbosity::Verbose);
        s.apply_part(&tool("bash", ToolStatus::Pending));
        assert_eq!(s.render(), "🔧 Working…\n⚙️ bash");
        s.apply_part(&tool_titled("bash", ToolStatus::Running, "git status"));
        assert_eq!(s.render(), "🔧 Working…\n⚙️ bash · git status");
    }

    #[test]
    fn footer_is_absent_during_live_render() {
        // The footer is a completion-only artifact — it never appears in the live
        // (streaming) render, only in finalize.
        let mut s = LiveState::new(Verbosity::Normal);
        s.apply_part(&tool_id("bash", "c1", ToolStatus::Completed));
        s.push_text("streaming answer");
        assert_eq!(s.render(), "🔧 Working…\n✓ bash\n\nstreaming answer");
        assert_eq!(
            s.finalize("streaming answer").body,
            "streaming answer\n✓ 1 tool"
        );
    }

    // --- intermediate narration (#6, Verbose) ------------------------------------

    #[test]
    fn verbose_echoes_narration_into_the_log_when_the_next_tool_starts() {
        let mut s = LiveState::new(Verbosity::Verbose);
        s.apply_part(&tool_id("bash", "c1", ToolStatus::Completed));
        s.push_text("Checking what's active…");
        // While streaming, the narration shows as the answer tail below the log.
        assert_eq!(s.render(), "🔧 Working…\n✓ bash\n\nChecking what's active…");
        // Once the next tool starts, it folds into the timeline as a 💬 line.
        s.apply_part(&tool_id("grep", "c2", ToolStatus::Running));
        assert_eq!(
            s.render(),
            "🔧 Working…\n✓ bash\n💬 Checking what's active…\n⚙️ grep"
        );
    }

    #[test]
    fn narration_stays_in_the_final_answer_and_in_the_collapsed_log() {
        let mut s = LiveState::new(Verbosity::Verbose);
        s.push_text("Checking…");
        s.apply_part(&tool_id("bash", "c1", ToolStatus::Completed));
        // The echo is a timeline marker only — the authoritative answer (which
        // contains the narration) is rendered untouched.
        let m = s.finalize("Checking…\n\nAll done.");
        assert_eq!(m.body, "Checking…\n\nAll done.\n✓ 1 tool");
        let log = m.log.expect("verbose folds the log");
        assert!(log.plain.contains("💬 Checking…"), "got: {}", log.plain);
    }

    #[test]
    fn normal_does_not_echo_narration() {
        let mut s = LiveState::new(Verbosity::Normal);
        s.push_text("Checking…");
        s.apply_part(&tool_id("bash", "c1", ToolStatus::Running));
        // No 💬 line at Normal; the streamed text stays in the answer area.
        assert_eq!(s.render(), "🔧 Working…\n⚙️ bash\n\nChecking…");
    }

    #[test]
    fn narration_is_clipped_to_its_first_line() {
        let mut s = LiveState::new(Verbosity::Verbose);
        s.push_text("First line of narration\nsecond line");
        s.apply_part(&tool_id("bash", "c1", ToolStatus::Running));
        assert_eq!(
            s.render(),
            "🔧 Working…\n💬 First line of narration…\n⚙️ bash"
        );
    }

    // --- final collapsed log (#6, Verbose) ----------------------------------------

    #[test]
    fn verbose_finalize_folds_the_log_into_an_expandable_blockquote() {
        let mut s = LiveState::new(Verbosity::Verbose).with_context_limit(Some(100_000));
        s.apply_part(&tool_id_titled(
            "bash",
            "c1",
            ToolStatus::Completed,
            "git status",
        ));
        s.apply_part(&tool_id_titled(
            "read",
            "c2",
            ToolStatus::Completed,
            "pantry.rs",
        ));
        s.set_context_used(8_000);
        let m = s.finalize("Here's what's in your pantry: …");
        // Body: clean answer, footer last.
        assert_eq!(m.body, "Here's what's in your pantry: …\n✓ 2 tools · 🧠 8%");
        // Log: `**>` first line, `>` continuation lines, `||` suffix on the last
        // — Telegram's expandable-blockquote markers — with the titles escaped
        // for MarkdownV2 (`.` → `\.`).
        let log = m.log.expect("verbose folds the log");
        assert_eq!(
            log.formatted,
            "**>🔧 2 tools\n>✓ bash · git status\n>✓ read · pantry\\.rs||"
        );
        assert_eq!(
            log.plain,
            "🔧 2 tools\n✓ bash · git status\n✓ read · pantry.rs"
        );
    }

    #[test]
    fn normal_and_quiet_finalize_without_a_log() {
        for v in [Verbosity::Normal, Verbosity::Quiet] {
            let mut s = LiveState::new(v);
            s.apply_part(&tool_id("bash", "c1", ToolStatus::Completed));
            assert_eq!(s.finalize("done").log, None, "no collapsed log at {v:?}");
        }
    }

    #[test]
    fn failed_tools_stay_outside_the_collapsed_log() {
        let mut s = LiveState::new(Verbosity::Verbose);
        s.apply_part(&tool_id("bash", "c1", ToolStatus::Error));
        let m = s.finalize("Couldn't run it.");
        // The ✗ line inside the log is the timeline; the failure notice itself is
        // in the body, never hidden behind the tap-to-expand (#6).
        assert_eq!(m.body, "Couldn't run it.\n✗ bash: failed\n✓ 1 tool");
        assert!(m.log.expect("log present").plain.contains("✗ bash"));
    }

    // --- rolling window / length cap (#6) ------------------------------------------

    #[test]
    fn log_window_rolls_with_an_earlier_marker() {
        let mut s = LiveState::new(Verbosity::Normal);
        for i in 0..12 {
            s.apply_part(&tool_id("bash", &format!("c{i}"), ToolStatus::Completed));
        }
        let rendered = s.render();
        // 12 entries → 4 hidden behind the marker, the trailing 8 shown.
        assert!(
            rendered.starts_with("🔧 Working…\n… +4 earlier\n✓ bash"),
            "got: {rendered}"
        );
        assert_eq!(rendered.matches("✓ bash").count(), 8);
    }

    #[test]
    fn collapsed_log_uses_the_same_window() {
        let mut s = LiveState::new(Verbosity::Verbose);
        for i in 0..12 {
            s.apply_part(&tool_id("bash", &format!("c{i}"), ToolStatus::Completed));
        }
        let log = s.finalize("done").log.expect("log present");
        assert!(
            log.formatted
                .starts_with("**>🔧 12 tools\n>… \\+4 earlier\n>✓ bash"),
            "got: {}",
            log.formatted
        );
        assert!(log.formatted.ends_with("||"));
        assert_eq!(log.formatted.matches("✓ bash").count(), 8);
    }

    #[test]
    fn long_titles_are_clipped_in_log_lines() {
        let mut s = LiveState::new(Verbosity::Verbose);
        let long = "x".repeat(200);
        s.apply_part(&tool_titled("bash", ToolStatus::Running, &long));
        let line = s.render();
        let expected = format!("🔧 Working…\n⚙️ bash · {}…", "x".repeat(64));
        assert_eq!(line, expected);
    }

    #[test]
    fn clip_keeps_short_single_lines_intact() {
        assert_eq!(clip("git status", 64), "git status");
        assert_eq!(clip("a\nb", 64), "a…");
        assert_eq!(clip(&"y".repeat(65), 64), format!("{}…", "y".repeat(64)));
        assert_eq!(clip("  padded  ", 64), "padded");
    }

    // --- context-usage footer (#72) --------------------------------------------

    #[test]
    fn context_percent_shows_when_the_limit_is_known() {
        // 42_000 / 100_000 = 42%, appended after the tool tally, footer last (#6).
        let mut s = LiveState::new(Verbosity::Normal).with_context_limit(Some(100_000));
        s.apply_part(&tool_id("bash", "c1", ToolStatus::Completed));
        s.set_context_used(42_000);
        assert_eq!(s.finalize("done").body, "done\n✓ 1 tool · 🧠 42%");
    }

    #[test]
    fn context_shows_on_a_tool_free_turn_without_a_checkmark() {
        // No tool ran, but context usage still surfaces (unlike the old gate) —
        // and with no `✓`, which marks the tool summary only (#72 feedback).
        let mut s = LiveState::new(Verbosity::Normal).with_context_limit(Some(200_000));
        s.set_context_used(50_000);
        assert_eq!(s.finalize("just text").body, "just text\n🧠 25%");
    }

    #[test]
    fn context_falls_back_to_raw_count_without_a_limit() {
        // No context-window configured → a human token count, not a %.
        let mut s = LiveState::new(Verbosity::Normal);
        s.set_context_used(12_345);
        assert_eq!(s.finalize("answer").body, "answer\n🧠 12.3k ctx");
    }

    #[test]
    fn context_hidden_in_quiet_mode() {
        let mut s = LiveState::new(Verbosity::Quiet).with_context_limit(Some(100_000));
        s.set_context_used(42_000);
        assert_eq!(s.finalize("done").body, "done");
    }

    #[test]
    fn no_footer_when_no_tools_and_no_context() {
        // Unchanged behaviour: a plain answer with nothing to report has no footer.
        let s = LiveState::new(Verbosity::Normal);
        assert_eq!(s.finalize("plain").body, "plain");
    }

    #[test]
    fn human_tokens_scales_units() {
        assert_eq!(human_tokens(950), "950");
        assert_eq!(human_tokens(12_345), "12.3k");
        assert_eq!(human_tokens(1_200_000), "1.2M");
    }
}
