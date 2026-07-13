//! opencode output → Telegram: 4096-char chunking, ≤1/sec stream-edit throttle,
//! `typing` liveness, flat tool-status line, and a completion summary footer (#14).
//! See `docs/design/architecture.md` §13. Issues #6/#8/#10/#14.
//!
//! [`split_message`] (the blocking-reply chunker) landed in #6. [`LiveState`] is
//! the B2 streaming render machine (#8): a pure state accumulator the streaming
//! turn driver (`telegram::stream`) feeds opencode events into, asking it what a
//! single Telegram message should currently show. It does **no** I/O — the driver
//! owns the throttle ticker and the `editMessageText` calls — so the layout and
//! coalescing logic are unit-testable without a `Bot`.

use crate::opencode::events::{PartKind, ToolStatus};

/// Telegram's hard per-message limit (characters).
pub const TELEGRAM_LIMIT: usize = 4096;

/// Per-user output verbosity (`/quiet` · `/verbose`, §13, #10). Persisted per
/// chat; the streaming renderer reads it to decide how much to surface. `Normal`
/// is the default. Concrete effects: the tool-status line and the completion
/// **summary footer** (#14) show at Normal/Verbose and are hidden at Quiet; tool
/// **failures** are always shown at every level. Verbose-only detail (full tool
/// args, cost, per-file names) is a fast-follow pending live wire validation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Verbosity {
    /// Answer (and failures) only — no live tool-status line.
    Quiet,
    /// Answer stream + flat tool-status line (the B2 behaviour).
    #[default]
    Normal,
    /// Like `Normal` today; reserved for extra detail (args, cost) later.
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

    /// Whether the live tool-status line should be shown at this level.
    fn shows_tool_line(self) -> bool {
        !matches!(self, Verbosity::Quiet)
    }
}

/// The streaming render state for one turn (Option A: answer-first, transient
/// tool line, failures always shown).
///
/// The driver pushes text deltas and tool-part updates in; [`render`](Self::render)
/// returns the text the live Telegram message should show **right now**:
///
/// - before any answer text, the current tool activity (`⚙️ bash`);
/// - once the answer streams, the answer itself;
/// - tool failures are appended and kept at every stage (`✗ bash: …`).
///
/// The driver edits the message on a ≤1/sec ticker and only when [`render`] has
/// changed, so this type carries no timing — just the content.
#[derive(Debug, Default, Clone)]
pub struct LiveState {
    /// Accumulated visible answer text (concatenated `text` deltas).
    answer: String,
    /// The current in-flight tool, shown while there is no answer text yet.
    active_tool: Option<String>,
    /// Tool failures, shown at every stage and preserved into the final render.
    failures: Vec<String>,
    /// The user's output verbosity (#10) — gates the tool-status line.
    verbosity: Verbosity,
    /// Tool `call_id`s that have reached a terminal state, so the summary counts
    /// (#14) tally each tool once across its pending→running→terminal updates.
    counted_calls: std::collections::HashSet<String>,
    /// Completion tallies for the summary footer (#14): total tools run, of which
    /// `task` sub-agents, and `edit`/`write` file edits.
    tools: usize,
    subagents: usize,
    files_edited: usize,
}

impl LiveState {
    /// A render state at the given output verbosity (#10).
    pub fn new(verbosity: Verbosity) -> Self {
        Self {
            verbosity,
            ..Self::default()
        }
    }

    /// Append a streamed text chunk to the answer buffer. A non-empty answer
    /// supersedes the transient tool line in [`render`].
    pub fn push_text(&mut self, delta: &str) {
        self.answer.push_str(delta);
    }

    /// Apply a `message.part.updated` tool lifecycle: set/clear the active-tool
    /// line and record failures (kept visible at every verbosity, §13). Non-tool
    /// parts are ignored here — text arrives via [`push_text`].
    pub fn apply_part(&mut self, kind: &PartKind) {
        if let PartKind::Tool {
            name,
            call_id,
            status,
        } = kind
        {
            match status {
                ToolStatus::Pending | ToolStatus::Running => {
                    self.active_tool = Some(name.clone());
                }
                ToolStatus::Completed => {
                    self.active_tool = None;
                    self.count_tool(name, call_id);
                }
                ToolStatus::Error => {
                    self.active_tool = None;
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

    /// The one-line completion summary footer (#14), or `None` when it should be
    /// omitted — in Quiet mode, or when no tool ran (a plain text answer needs no
    /// summary). Shown above the answer on finalize (§13); zero categories are
    /// dropped, e.g. `✓ 3 tools · edited 1 file`.
    fn summary_footer(&self) -> Option<String> {
        if matches!(self.verbosity, Verbosity::Quiet) || self.tools == 0 {
            return None;
        }
        let mut parts = vec![plural(self.tools, "tool", "tools")];
        if self.subagents > 0 {
            parts.push(plural(self.subagents, "subagent", "subagents"));
        }
        if self.files_edited > 0 {
            parts.push(format!(
                "edited {}",
                plural(self.files_edited, "file", "files")
            ));
        }
        Some(format!("✓ {}", parts.join(" · ")))
    }

    /// Whether the tool-status line is visible at the current verbosity and a
    /// tool is active (hidden entirely in Quiet, #10).
    fn tool_line_visible(&self) -> bool {
        self.verbosity.shows_tool_line() && self.active_tool.is_some()
    }

    /// Whether any visible content exists yet (answer, a shown tool line, or a
    /// failure) — the driver uses this to defer creating the Telegram message
    /// until there is something to show.
    pub fn has_content(&self) -> bool {
        !self.answer.is_empty() || self.tool_line_visible() || !self.failures.is_empty()
    }

    /// The full text the live message should show now — answer (or the transient
    /// tool line before any answer, unless Quiet) plus any failure lines. May
    /// exceed [`TELEGRAM_LIMIT`]; the driver clamps to the first chunk via
    /// [`split_message`] while streaming and chunks the rest on finalize.
    pub fn render(&self) -> String {
        let mut out = String::new();
        if !self.answer.is_empty() {
            out.push_str(&self.answer);
        } else if self.tool_line_visible() {
            out.push_str("⚙️ ");
            out.push_str(self.active_tool.as_deref().unwrap_or_default());
        }
        for failure in &self.failures {
            if !out.is_empty() {
                out.push('\n');
            }
            out.push_str(failure);
        }
        out
    }

    /// The authoritative answer text (no tool decoration) for the final render,
    /// with the completion summary footer (#14) above it and failures appended so
    /// a blocked command is never silently dropped.
    pub fn finalize(&self, authoritative: &str) -> String {
        let mut out = String::new();
        if let Some(footer) = self.summary_footer() {
            out.push_str(&footer);
            out.push('\n');
        }
        out.push_str(authoritative);
        for failure in &self.failures {
            if !out.is_empty() {
                out.push('\n');
            }
            out.push_str(failure);
        }
        out
    }
}

/// `"{n} {singular}"` or `"{n} {plural}"` by count — e.g. `plural(1, "file",
/// "files") == "1 file"`, `plural(2, …) == "2 files"`.
fn plural(n: usize, singular: &str, plural: &str) -> String {
    format!("{n} {}", if n == 1 { singular } else { plural })
}

/// Split `text` into pieces of at most `limit` characters, cutting only on
/// character boundaries (never mid-multibyte-char).
///
/// When a piece must be cut, we prefer to break at the last newline within the
/// final ~10% of the window so we don't slice a line in half; if there is no
/// newline there, we hard-cut at exactly `limit` characters.
///
/// An empty input yields an empty `Vec` (no message to send). A `limit` of 0 is
/// degenerate and returns the whole text as a single chunk rather than looping.
pub fn split_message(text: &str, limit: usize) -> Vec<String> {
    if text.is_empty() {
        return Vec::new();
    }
    if limit == 0 {
        return vec![text.to_string()];
    }

    let mut chunks = Vec::new();
    let mut rest = text;
    loop {
        if rest.chars().count() <= limit {
            chunks.push(rest.to_string());
            break;
        }

        // Byte offset just after the `limit`-th char (the hard cut point).
        let hard = byte_offset_of_char(rest, limit);

        // Look for a newline in the last ~10% of the window and break there.
        let window_chars = (limit / 10).max(1);
        let window_start = byte_offset_of_char(rest, limit - window_chars);
        let split_at = rest[window_start..hard]
            .rfind('\n')
            .map(|pos| window_start + pos + 1)
            .unwrap_or(hard);

        chunks.push(rest[..split_at].to_string());
        rest = &rest[split_at..];
    }
    chunks
}

/// Byte offset of the `n`-th character in `s` (0-indexed), or `s.len()` if `s`
/// has `n` characters or fewer. The result is always a char boundary.
fn byte_offset_of_char(s: &str, n: usize) -> usize {
    s.char_indices().nth(n).map_or(s.len(), |(i, _)| i)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_yields_no_chunks() {
        assert!(split_message("", TELEGRAM_LIMIT).is_empty());
    }

    #[test]
    fn short_text_is_one_chunk() {
        let out = split_message("hello world", TELEGRAM_LIMIT);
        assert_eq!(out, vec!["hello world".to_string()]);
    }

    #[test]
    fn text_exactly_at_limit_is_one_chunk() {
        let text = "a".repeat(10);
        let out = split_message(&text, 10);
        assert_eq!(out, vec![text]);
    }

    #[test]
    fn every_chunk_within_limit_and_reconstructs() {
        let limit = 100;
        let text = "abcdefghij".repeat(95); // 950 chars, no newlines
        let out = split_message(&text, limit);
        assert!(out.len() > 1, "long text must split");
        for chunk in &out {
            assert!(chunk.chars().count() <= limit, "chunk over limit");
        }
        assert_eq!(out.concat(), text, "chunks must reconstruct input");
    }

    #[test]
    fn prefers_newline_break_near_the_end() {
        // Limit 20; a newline sits at char 18 (within the last 10% window).
        let text = "0123456789abcdefgh\nrest of the second line here";
        let out = split_message(text, 20);
        assert!(out[0].ends_with('\n'), "first chunk should end at newline");
        assert_eq!(out.concat(), text);
    }

    #[test]
    fn multibyte_is_boundary_safe_and_reconstructs() {
        // Mix of 1-, 3- and 4-byte chars; force many splits.
        let text = "aé中🚀".repeat(50); // 200 chars, 10 bytes per repeat
        let out = split_message(&text, 7);
        for chunk in &out {
            assert!(chunk.chars().count() <= 7);
            // If this ever cut mid-char the slice would have panicked already,
            // but assert the round-trip to be explicit.
        }
        assert_eq!(out.concat(), text);
    }

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
        }
    }

    #[test]
    fn live_state_shows_tool_line_before_any_answer() {
        let mut s = LiveState::new(Verbosity::Normal);
        assert!(!s.has_content());
        s.apply_part(&tool("bash", ToolStatus::Running));
        assert!(s.has_content());
        assert_eq!(s.render(), "⚙️ bash");
    }

    #[test]
    fn quiet_hides_the_tool_line_but_not_failures_or_answer() {
        let mut s = LiveState::new(Verbosity::Quiet);
        // A running tool produces no visible content in Quiet mode.
        s.apply_part(&tool("bash", ToolStatus::Running));
        assert!(!s.has_content(), "quiet mode hides the tool-status line");
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
    fn live_state_answer_supersedes_tool_line() {
        let mut s = LiveState::new(Verbosity::Normal);
        s.apply_part(&tool("bash", ToolStatus::Running));
        s.push_text("The output ");
        s.push_text("is hi.");
        // Once answer text exists, the transient tool line is gone.
        assert_eq!(s.render(), "The output is hi.");
    }

    #[test]
    fn live_state_completed_tool_clears_line() {
        let mut s = LiveState::new(Verbosity::Normal);
        s.apply_part(&tool("bash", ToolStatus::Pending));
        assert_eq!(s.render(), "⚙️ bash");
        s.apply_part(&tool("bash", ToolStatus::Completed));
        // No answer, no active tool → nothing to show yet.
        assert!(!s.has_content());
        assert_eq!(s.render(), "");
    }

    #[test]
    fn live_state_failures_always_shown() {
        let mut s = LiveState::new(Verbosity::Normal);
        s.apply_part(&tool("bash", ToolStatus::Error));
        // A failure surfaces even with no answer text.
        assert_eq!(s.render(), "✗ bash: failed");
        s.push_text("The command was blocked.");
        assert_eq!(s.render(), "The command was blocked.\n✗ bash: failed");
        // Dedup: the same failure is not repeated.
        s.apply_part(&tool("bash", ToolStatus::Error));
        assert_eq!(s.render().matches("✗ bash").count(), 1);
    }

    #[test]
    fn live_state_finalize_appends_failures_to_authoritative_text() {
        let mut s = LiveState::new(Verbosity::Normal);
        s.push_text("partial…"); // streamed buffer is ignored by finalize
        s.apply_part(&tool("bash", ToolStatus::Error));
        // The errored tool both counts toward the footer and lists its failure.
        assert_eq!(
            s.finalize("The full answer."),
            "✓ 1 tool\nThe full answer.\n✗ bash: failed"
        );
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
            s.finalize("Done."),
            "✓ 6 tools · 1 subagent · edited 2 files\nDone."
        );
    }

    #[test]
    fn footer_omits_zero_categories_and_singularizes() {
        let mut s = LiveState::new(Verbosity::Normal);
        s.apply_part(&tool_id("edit", "c1", ToolStatus::Completed));
        // One edit tool: "1 tool" (singular) + "edited 1 file"; no subagent clause.
        assert_eq!(s.finalize("ok"), "✓ 1 tool · edited 1 file\nok");
    }

    #[test]
    fn footer_hidden_in_quiet_and_absent_without_tools() {
        // Quiet: no footer even though a tool ran.
        let mut q = LiveState::new(Verbosity::Quiet);
        q.apply_part(&tool_id("bash", "c1", ToolStatus::Completed));
        assert_eq!(q.finalize("ans"), "ans");
        // Normal but no tools: a plain text answer gets no footer.
        let plain = LiveState::new(Verbosity::Normal);
        assert_eq!(plain.finalize("just text"), "just text");
    }

    #[test]
    fn footer_tallies_each_call_once_across_updates() {
        let mut s = LiveState::new(Verbosity::Normal);
        // The same call_id transitions pending → running → completed: counted once.
        s.apply_part(&tool_id("bash", "c1", ToolStatus::Pending));
        s.apply_part(&tool_id("bash", "c1", ToolStatus::Running));
        s.apply_part(&tool_id("bash", "c1", ToolStatus::Completed));
        s.apply_part(&tool_id("bash", "c1", ToolStatus::Completed)); // duplicate terminal
        assert_eq!(s.finalize("done"), "✓ 1 tool\ndone");
    }

    #[test]
    fn footer_is_absent_during_live_render() {
        // The footer is a completion-only artifact — it never appears in the live
        // (streaming) render, only in finalize.
        let mut s = LiveState::new(Verbosity::Normal);
        s.apply_part(&tool_id("bash", "c1", ToolStatus::Completed));
        s.push_text("streaming answer");
        assert_eq!(s.render(), "streaming answer");
        assert_eq!(s.finalize("streaming answer"), "✓ 1 tool\nstreaming answer");
    }
}
