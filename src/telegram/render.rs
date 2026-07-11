//! opencode output → Telegram: 4096-char chunking, ≤1/sec stream-edit throttle,
//! `typing` liveness, flat tool-status line, summary footer.
//! See `docs/design/architecture.md` §13. Issues #6/#8.
//!
//! Only [`split_message`] (the blocking-reply chunker) lands in #6; the
//! stream-edit throttle and status line land with SSE streaming (#7/#8).

/// Telegram's hard per-message limit (characters).
pub const TELEGRAM_LIMIT: usize = 4096;

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
}
