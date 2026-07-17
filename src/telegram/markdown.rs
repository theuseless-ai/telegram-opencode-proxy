//! Model Markdown → Telegram **MarkdownV2** (issue #70).
//!
//! The model answers in ordinary (GitHub-flavoured) Markdown, but Telegram will
//! only *render* formatting when a message is sent with `parse_mode=MarkdownV2`
//! — and MarkdownV2 is unforgiving: every literal `_ * [ ] ( ) ~ ` > # + - = |
//! { } . !` in prose must be backslash-escaped or the whole message is rejected
//! with `can't parse entities`. Rather than trust the model to escape perfectly,
//! we convert deterministically: parse the Markdown with `pulldown-cmark` and
//! re-emit it as MarkdownV2, escaping **only the text runs** while writing the
//! markup characters (`*`, `_`, `` ` ``, …) raw. This is the same strategy the
//! `telegramify-markdown` library takes (it wraps the very same parser).
//!
//! The conversion is a pure function ([`to_telegram`]) so it is unit-testable
//! without a `Bot`, matching the rest of `telegram::render`. Because a real
//! CommonMark parse always yields *balanced* structure — an unclosed `**` or a
//! dangling code fence is treated as literal text and thus escaped, not left as
//! stray markup — the output is valid MarkdownV2 for **any** input, including the
//! partial prefixes produced while a turn is still streaming.
//!
//! Constructs Telegram has no markup for are downgraded: headings → bold lines,
//! lists → `•` / `N.` text, tables → a monospace (`pre`) block, thematic breaks
//! → a rule line. Anything that still slips through to an invalid message is
//! caught by the caller's plain-text fallback (`telegram::stream`).

use pulldown_cmark::{Event, HeadingLevel, Options, Parser, Tag, TagEnd};

/// A rendered message chunk: the MarkdownV2 [`formatted`](Self::formatted) body
/// to send with `parse_mode=MarkdownV2`, paired with the raw-Markdown
/// [`plain`](Self::plain) source it came from — the caller sends `plain` with no
/// parse mode if Telegram rejects `formatted` (graceful degradation, #70).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Chunk {
    /// MarkdownV2 body, safe to send with `parse_mode=MarkdownV2`.
    pub formatted: String,
    /// The original Markdown source for this chunk — the plain-text fallback.
    pub plain: String,
}

/// The MarkdownV2-reserved characters that must be `\`-escaped in ordinary text
/// (Telegram Bot API §MarkdownV2).
const RESERVED: &[char] = &[
    '_', '*', '[', ']', '(', ')', '~', '`', '>', '#', '+', '-', '=', '|', '{', '}', '.', '!',
];

/// Convert a Markdown document to a single MarkdownV2 string.
///
/// Always returns valid MarkdownV2 (balanced entities, all literal text escaped),
/// even for a partial/streaming prefix. May exceed Telegram's length limit; use
/// [`to_chunks`] when the result must fit one message.
pub fn to_telegram(md: &str) -> String {
    let mut opts = Options::empty();
    opts.insert(Options::ENABLE_STRIKETHROUGH);
    opts.insert(Options::ENABLE_TABLES);
    opts.insert(Options::ENABLE_TASKLISTS);
    let parser = Parser::new_ext(md, opts);

    let mut w = Writer::default();
    for event in parser {
        w.event(event);
    }
    w.finish()
}

/// Split a Markdown document into MarkdownV2 [`Chunk`]s each at most `limit`
/// characters once rendered, so every piece is a valid, sendable message.
///
/// Chunking happens on the **source** at line boundaries and each piece is
/// converted independently, so an entity never straddles a chunk edge — a run cut
/// mid-`**bold**` simply renders as escaped literal text on both sides. An empty
/// document yields no chunks.
pub fn to_chunks(md: &str, limit: usize) -> Vec<Chunk> {
    if md.is_empty() {
        return Vec::new();
    }
    // Fast path: the whole document already fits.
    let whole = to_telegram(md);
    if whole.chars().count() <= limit {
        return vec![Chunk {
            formatted: whole,
            plain: md.to_string(),
        }];
    }

    let mut chunks = Vec::new();
    let mut buf = String::new();
    let flush = |chunks: &mut Vec<Chunk>, buf: &mut String| {
        if buf.is_empty() {
            return;
        }
        chunks.push(Chunk {
            formatted: to_telegram(buf),
            plain: std::mem::take(buf),
        });
    };

    for line in md.split_inclusive('\n') {
        // A single line whose rendering alone blows the limit: emit whatever is
        // buffered, then hard-split the oversized line on char boundaries.
        if to_telegram(line).chars().count() > limit {
            flush(&mut chunks, &mut buf);
            for piece in hard_split(line, limit) {
                chunks.push(Chunk {
                    formatted: to_telegram(&piece),
                    plain: piece,
                });
            }
            continue;
        }
        let candidate = format!("{buf}{line}");
        if to_telegram(&candidate).chars().count() > limit {
            flush(&mut chunks, &mut buf); // `line` starts the next chunk.
            buf.push_str(line);
        } else {
            buf = candidate;
        }
    }
    flush(&mut chunks, &mut buf);
    chunks
}

/// Split `s` into `limit`-char pieces on char boundaries (last-ditch fallback for
/// a single source line that renders too long to fit one message).
fn hard_split(s: &str, limit: usize) -> Vec<String> {
    let limit = limit.max(1);
    let mut pieces = Vec::new();
    let mut piece = String::new();
    for c in s.chars() {
        if piece.chars().count() >= limit {
            pieces.push(std::mem::take(&mut piece));
        }
        piece.push(c);
    }
    if !piece.is_empty() {
        pieces.push(piece);
    }
    pieces
}

/// Escape every MarkdownV2-reserved char in an ordinary text run.
fn escape_text(out: &mut String, s: &str) {
    for c in s.chars() {
        if RESERVED.contains(&c) {
            out.push('\\');
        }
        out.push(c);
    }
}

/// [`escape_text`] as a returning function — for callers assembling small
/// MarkdownV2 fragments by hand (the activity-log lines, #6) rather than
/// converting a whole Markdown document through [`to_telegram`].
pub fn escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    escape_text(&mut out, s);
    out
}

/// Build a Telegram **expandable blockquote** (Bot API 7.0) from pre-escaped
/// MarkdownV2 lines: the first line is prefixed `**>`, every subsequent line
/// `>`, and the very last line is suffixed `||` — Telegram renders the block
/// collapsed with a tap-to-expand chevron. Used to fold a turn's activity log
/// under the answer on finalize (#6). Lines must already be MarkdownV2-safe
/// ([`escape`]); an empty slice yields an empty string.
pub fn expandable_quote(lines: &[String]) -> String {
    let mut out = String::new();
    for (i, line) in lines.iter().enumerate() {
        if i == 0 {
            out.push_str("**>");
        } else {
            out.push_str("\n>");
        }
        out.push_str(line);
    }
    if !out.is_empty() {
        out.push_str("||");
    }
    out
}

/// Escape a run destined for a `code`/`pre` entity: only `` ` `` and `\` are
/// special there.
fn escape_code(out: &mut String, s: &str) {
    for c in s.chars() {
        if c == '`' || c == '\\' {
            out.push('\\');
        }
        out.push(c);
    }
}

/// Escape a link/image URL: inside `(...)` only `)` and `\` are special.
fn escape_url(out: &mut String, s: &str) {
    for c in s.chars() {
        if c == ')' || c == '\\' {
            out.push('\\');
        }
        out.push(c);
    }
}

/// One open ordered/unordered list level; `Some(n)` tracks the next ordinal.
type ListLevel = Option<u64>;

/// Streaming MarkdownV2 writer folded over the `pulldown-cmark` event stream.
#[derive(Default)]
struct Writer {
    out: String,
    /// Destination URLs for currently-open links/images (LIFO).
    links: Vec<String>,
    /// Open list levels; the innermost is last. Drives item markers + indent.
    lists: Vec<ListLevel>,
    /// `out` indices where an open block-quote began, for line-prefixing at end.
    quotes: Vec<usize>,
    /// Inside a fenced/indented code block, where text is raw code — only `` ` ``
    /// and `\` are escaped, not the full reserved set.
    in_code_block: bool,
    /// When inside a table, the accumulating grid and current cell.
    table: Option<Table>,
}

/// A table being accumulated as plain cell text, rendered as a monospace block.
#[derive(Default)]
struct Table {
    rows: Vec<Vec<String>>,
    cell: String,
    in_cell: bool,
}

impl Writer {
    fn finish(mut self) -> String {
        // Trim the trailing block separator we leave after each block.
        while self.out.ends_with('\n') {
            self.out.pop();
        }
        self.out
    }

    /// Ensure `out` ends with a blank line (block separator), unless it is empty.
    fn ensure_blank(&mut self) {
        if self.out.is_empty() {
            return;
        }
        if !self.out.ends_with('\n') {
            self.out.push('\n');
        }
        if !self.out.ends_with("\n\n") {
            self.out.push('\n');
        }
    }

    /// Ensure `out` ends with at least one newline (unless empty).
    fn ensure_nl(&mut self) {
        if !self.out.is_empty() && !self.out.ends_with('\n') {
            self.out.push('\n');
        }
    }

    fn event(&mut self, event: Event) {
        // While buffering a table, route text/structure into the grid.
        if self.table.is_some() && self.table_event(&event) {
            return;
        }
        match event {
            Event::Start(tag) => self.start(tag),
            Event::End(tag) => self.end(tag),
            Event::Text(t) if self.in_code_block => escape_code(&mut self.out, &t),
            Event::Text(t) => escape_text(&mut self.out, &t),
            Event::Code(t) => {
                self.out.push('`');
                escape_code(&mut self.out, &t);
                self.out.push('`');
            }
            Event::SoftBreak | Event::HardBreak => self.out.push('\n'),
            Event::Rule => {
                self.ensure_blank();
                self.out.push_str("──────────");
                self.ensure_blank();
            }
            Event::TaskListMarker(done) => {
                self.out
                    .push_str(if done { "\\[x\\] " } else { "\\[ \\] " });
            }
            // Raw HTML in the model's Markdown has no MarkdownV2 equivalent; emit
            // it as escaped literal text rather than smuggling tags through.
            Event::Html(h) | Event::InlineHtml(h) => escape_text(&mut self.out, &h),
            _ => {}
        }
    }

    fn start(&mut self, tag: Tag) {
        match tag {
            // Inside a list item (a "loose" list wraps item text in paragraphs)
            // the text flows right after the marker — no leading blank line.
            Tag::Paragraph if !self.lists.is_empty() => {}
            Tag::Paragraph => self.ensure_blank(),
            Tag::Heading { level, .. } => {
                self.ensure_blank();
                // Telegram has no headings; bold the line, with a scaling marker.
                let hashes = "#".repeat(heading_depth(level));
                // The marker is decorative text, so escape it.
                escape_text(&mut self.out, &hashes);
                if !hashes.is_empty() {
                    self.out.push(' ');
                }
                self.out.push('*');
            }
            Tag::BlockQuote(_) => {
                self.ensure_blank();
                self.quotes.push(self.out.len());
            }
            Tag::CodeBlock(kind) => {
                self.ensure_blank();
                self.out.push_str("```");
                if let pulldown_cmark::CodeBlockKind::Fenced(lang) = kind {
                    let lang = lang.trim();
                    if !lang.is_empty() {
                        // Language token is bare (no escaping); newline ends it.
                        self.out.push_str(lang);
                    }
                }
                self.out.push('\n');
                self.in_code_block = true;
            }
            Tag::List(first) => {
                // A nested list continues on the next line; only a top-level list
                // is preceded by a blank line.
                if self.lists.is_empty() {
                    self.ensure_blank();
                } else {
                    self.ensure_nl();
                }
                self.lists.push(first);
            }
            Tag::Item => {
                self.ensure_nl();
                let depth = self.lists.len().saturating_sub(1);
                for _ in 0..depth {
                    self.out.push_str("  ");
                }
                match self.lists.last_mut() {
                    Some(Some(n)) => {
                        // Ordered: "N." — the dot is reserved, so escape it.
                        self.out.push_str(&n.to_string());
                        self.out.push_str("\\. ");
                        *n += 1;
                    }
                    _ => self.out.push_str("• "),
                }
            }
            Tag::Emphasis => self.out.push('_'),
            Tag::Strong => self.out.push('*'),
            Tag::Strikethrough => self.out.push('~'),
            Tag::Link { dest_url, .. } | Tag::Image { dest_url, .. } => {
                self.out.push('[');
                self.links.push(dest_url.into_string());
            }
            Tag::Table(_) => {
                self.ensure_blank();
                self.table = Some(Table::default());
            }
            _ => {}
        }
    }

    fn end(&mut self, tag: TagEnd) {
        match tag {
            // Inside a list, keep item paragraphs on their own line rather than
            // separating them with a blank line (see the matching `start`).
            TagEnd::Paragraph if !self.lists.is_empty() => self.ensure_nl(),
            TagEnd::Paragraph => self.ensure_blank(),
            TagEnd::Heading(_) => {
                self.out.push('*');
                self.ensure_blank();
            }
            TagEnd::BlockQuote(_) => {
                if let Some(start) = self.quotes.pop() {
                    self.prefix_quote(start);
                }
                self.ensure_blank();
            }
            TagEnd::CodeBlock => {
                self.in_code_block = false;
                self.ensure_nl();
                self.out.push_str("```");
                self.ensure_blank();
            }
            TagEnd::List(_) => {
                self.lists.pop();
                self.ensure_blank();
            }
            TagEnd::Item => self.ensure_nl(),
            TagEnd::Emphasis => self.out.push('_'),
            TagEnd::Strong => self.out.push('*'),
            TagEnd::Strikethrough => self.out.push('~'),
            TagEnd::Link | TagEnd::Image => {
                self.out.push_str("](");
                if let Some(url) = self.links.pop() {
                    escape_url(&mut self.out, &url);
                }
                self.out.push(')');
            }
            _ => {}
        }
    }

    /// Prefix every line of the block-quote body (from index `start`) with `>`,
    /// Telegram's MarkdownV2 quote markup.
    fn prefix_quote(&mut self, start: usize) {
        let body = self.out.split_off(start);
        let trimmed = body.trim_matches('\n');
        for (i, line) in trimmed.split('\n').enumerate() {
            if i > 0 {
                self.out.push('\n');
            }
            self.out.push('>');
            self.out.push_str(line);
        }
    }

    /// Route an event into the in-progress table grid. Returns `true` if the
    /// event was consumed (i.e. we are inside table markup).
    fn table_event(&mut self, event: &Event) -> bool {
        let table = self.table.as_mut().expect("called only when table is Some");
        match event {
            Event::Start(Tag::TableHead | Tag::TableRow) => {
                table.rows.push(Vec::new());
                true
            }
            Event::Start(Tag::TableCell) => {
                table.cell.clear();
                table.in_cell = true;
                true
            }
            Event::End(TagEnd::TableCell) => {
                let cell = std::mem::take(&mut table.cell);
                if let Some(row) = table.rows.last_mut() {
                    row.push(cell);
                }
                table.in_cell = false;
                true
            }
            Event::Text(t) | Event::Code(t) if table.in_cell => {
                table.cell.push_str(t);
                true
            }
            Event::End(TagEnd::Table) => {
                let table = self.table.take().expect("table present");
                self.render_table(table);
                true
            }
            // Swallow the structural head/row ends and anything else inside.
            Event::End(TagEnd::TableHead | TagEnd::TableRow) => true,
            _ => table.in_cell, // ignore stray inline markup within a cell
        }
    }

    /// Render an accumulated table as a left-aligned monospace `pre` block.
    fn render_table(&mut self, table: Table) {
        let cols = table.rows.iter().map(Vec::len).max().unwrap_or(0);
        let mut widths = vec![0usize; cols];
        for row in &table.rows {
            for (i, cell) in row.iter().enumerate() {
                widths[i] = widths[i].max(cell.chars().count());
            }
        }
        self.ensure_blank();
        self.out.push_str("```\n");
        for row in &table.rows {
            let mut line = String::new();
            for (i, width) in widths.iter().enumerate() {
                let cell = row.get(i).map(String::as_str).unwrap_or("");
                let pad = width.saturating_sub(cell.chars().count());
                if i > 0 {
                    line.push_str(" | ");
                }
                line.push_str(cell);
                line.push_str(&" ".repeat(pad));
            }
            escape_code(&mut self.out, line.trim_end());
            self.out.push('\n');
        }
        self.out.push_str("```");
        self.ensure_blank();
    }
}

/// Heading depth 1..=6 as a small count (drives the `#` prefix on bold lines).
fn heading_depth(level: HeadingLevel) -> usize {
    match level {
        HeadingLevel::H1 => 1,
        HeadingLevel::H2 => 2,
        HeadingLevel::H3 => 3,
        HeadingLevel::H4 => 4,
        HeadingLevel::H5 => 5,
        HeadingLevel::H6 => 6,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn md(s: &str) -> String {
        to_telegram(s)
    }

    #[test]
    fn plain_prose_escapes_reserved_chars() {
        // Every reserved char in prose must be backslash-escaped.
        assert_eq!(
            md("Cost is 3.5 (approx) - see item #2!"),
            "Cost is 3\\.5 \\(approx\\) \\- see item \\#2\\!"
        );
    }

    #[test]
    fn bold_italic_strike_code_use_raw_markup() {
        assert_eq!(md("**bold**"), "*bold*");
        assert_eq!(md("*italic*"), "_italic_");
        assert_eq!(md("~~gone~~"), "~gone~");
        assert_eq!(md("`x = 1.0`"), "`x = 1.0`"); // inside code, `.` is literal
    }

    #[test]
    fn text_inside_bold_is_still_escaped() {
        assert_eq!(md("**a.b**"), "*a\\.b*");
    }

    #[test]
    fn inline_code_escapes_only_backtick_and_backslash() {
        assert_eq!(md(r"`a\b`"), r"`a\\b`");
    }

    #[test]
    fn fenced_code_block_keeps_language_and_escapes_body() {
        let out = md("```rust\nlet x = 1; // a-b\n```");
        assert_eq!(out, "```rust\nlet x = 1; // a-b\n```");
    }

    #[test]
    fn links_emit_markup_and_escape_url() {
        assert_eq!(
            md("[docs](https://ex.com/a(b))"),
            "[docs](https://ex.com/a(b\\))"
        );
    }

    #[test]
    fn heading_becomes_bold_line() {
        assert_eq!(md("## Title"), "\\#\\# *Title*");
    }

    #[test]
    fn unordered_list_uses_bullets() {
        assert_eq!(md("- one\n- two"), "• one\n• two");
    }

    #[test]
    fn ordered_list_numbers_with_escaped_dot() {
        assert_eq!(md("1. one\n2. two"), "1\\. one\n2\\. two");
    }

    #[test]
    fn loose_list_items_stay_on_their_own_line() {
        // Blank-separated ("loose") items are wrapped in paragraphs by the parser;
        // the marker and text must not be split across a blank line.
        assert_eq!(md("- one\n\n- two"), "• one\n• two");
    }

    #[test]
    fn nested_list_indents_and_bullets() {
        assert_eq!(md("- a\n  - b"), "• a\n  • b");
    }

    #[test]
    fn blockquote_prefixes_each_line() {
        // Telegram MarkdownV2 quotes prefix *every* line with `>`.
        assert_eq!(md("> hi\n> there"), ">hi\n>there");
    }

    #[test]
    fn partial_unclosed_bold_is_valid_escaped_text() {
        // A streaming prefix ending mid-bold must not leave a stray `*`.
        assert_eq!(md("see **bold"), "see \\*\\*bold");
    }

    #[test]
    fn partial_unclosed_code_fence_closes_cleanly() {
        // An open fence renders as a closed code block (parser closes it at EOF).
        assert_eq!(md("```\ncode"), "```\ncode\n```");
    }

    #[test]
    fn empty_document_yields_no_chunks() {
        assert!(to_chunks("", 100).is_empty());
    }

    #[test]
    fn short_document_is_one_chunk_with_plain_fallback() {
        let chunks = to_chunks("hello.", 4096);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].formatted, "hello\\.");
        assert_eq!(chunks[0].plain, "hello.");
    }

    #[test]
    fn long_document_splits_into_valid_bounded_chunks() {
        // 60 lines, forced to split at a small limit.
        let src: String = (0..60).map(|i| format!("line {i}.\n")).collect();
        let chunks = to_chunks(&src, 80);
        assert!(chunks.len() > 1, "must split");
        for c in &chunks {
            assert!(c.formatted.chars().count() <= 80, "chunk within limit");
        }
        // Fallbacks reconstruct the original source exactly.
        assert_eq!(
            chunks.iter().map(|c| c.plain.clone()).collect::<String>(),
            src
        );
    }

    #[test]
    fn table_renders_as_monospace_block() {
        let out = md("| a | b |\n|---|---|\n| 1 | 22 |");
        assert_eq!(out, "```\na | b\n1 | 22\n```");
    }

    // --- expandable blockquote + escape helper (#6) -----------------------------

    #[test]
    fn escape_covers_every_reserved_char() {
        assert_eq!(escape("a.b-c"), "a\\.b\\-c");
        // The full reserved set round-trips with a backslash before each.
        for c in super::RESERVED {
            assert_eq!(escape(&c.to_string()), format!("\\{c}"));
        }
        assert_eq!(escape("plain"), "plain");
    }

    #[test]
    fn expandable_quote_uses_the_bot_api_markers() {
        // First line `**>`, subsequent `>`, last line suffixed `||`.
        let lines = vec!["🔧 2 tools".into(), "✓ bash".into(), "✓ read".into()];
        assert_eq!(
            expandable_quote(&lines),
            "**>🔧 2 tools\n>✓ bash\n>✓ read||"
        );
    }

    #[test]
    fn expandable_quote_single_line_and_empty() {
        assert_eq!(expandable_quote(&["only".to_string()]), "**>only||");
        assert_eq!(expandable_quote(&[]), "");
    }
}
