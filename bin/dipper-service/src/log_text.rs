//! Text written by someone outside dipper, such as the URL an indexer registers or the free text
//! in an indexer's reply, in a form that is safe to log.

/// The most characters of such a text that are logged.
const MAX_LOGGED_CHARS: usize = 200;

/// `raw` quoted with its control characters escaped, so a line break in it can't start a fake log
/// line, and cut short, so a huge one can't flood the log each time it is logged.
pub(crate) fn untrusted(raw: &str) -> String {
    let shown: String = raw.chars().take(MAX_LOGGED_CHARS).collect();
    if shown.len() < raw.len() {
        format!("{shown:?}...")
    } else {
        format!("{shown:?}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_short_text_is_quoted_whole() {
        assert_eq!(untrusted("price too low"), r#""price too low""#);
    }

    #[test]
    fn characters_that_could_forge_or_disguise_a_log_line_are_escaped() {
        // A line feed, a carriage return, the escape that starts a terminal colour code, and the
        // mark that makes text read right to left.
        let shown = untrusted("a\nb\rc\u{1b}[31md\u{202e}e");

        assert_eq!(shown, r#""a\nb\rc\u{1b}[31md\u{202e}e""#);
    }

    #[test]
    fn a_long_text_is_cut_between_characters_and_marked() {
        let exactly = "é".repeat(MAX_LOGGED_CHARS);
        let over = "é".repeat(MAX_LOGGED_CHARS + 1);

        assert_eq!(untrusted(&exactly), format!("{exactly:?}"));
        assert_eq!(untrusted(&over), format!("{exactly:?}..."));
    }
}
