//! Bounded retry / backoff for outbound Telegram Bot API calls (#25).
//!
//! Telegram enforces flood limits (~1 edit/sec per chat, ~30 msg/sec globally)
//! and answers a breach with HTTP 429 + `retry_after`; teloxide surfaces that as
//! [`RequestError::RetryAfter`]. Transient connectivity blips surface as
//! [`RequestError::Network`]. [`with_retry`] wraps a Telegram call so that:
//!
//! - a **flood** (`RetryAfter`) waits exactly as long as the server asks (capped),
//! - a **network** error backs off exponentially,
//! - anything else (a real API error, bad JSON, I/O) is **not** retried, and
//! - all paths give up after [`MAX_ATTEMPTS`], returning the error to the caller
//!   (which logs and moves on rather than crashing the turn).
//!
//! The live streaming edits already self-throttle to ≤1/sec (§13); this is the
//! safety net for the residual bursts (many chats at once, a transient blip).
//! A heavier alternative — teloxide's `Throttle` adaptor — would rate-limit at
//! the `Bot` layer but change the bot type throughout; this targeted wrapper is
//! enough for the 2-user daily driver.

use std::future::Future;
use std::time::Duration;

use teloxide::RequestError;

/// Total attempts (the initial call plus retries) before giving up.
const MAX_ATTEMPTS: u32 = 5;
/// Base delay for the exponential network-error backoff (doubles per attempt).
const NETWORK_BASE: Duration = Duration::from_millis(200);
/// Ceiling on any single wait — also caps a server-supplied `retry_after` so a
/// pathological value can't wedge a user's serial worker indefinitely.
const MAX_BACKOFF: Duration = Duration::from_secs(60);

/// Run `op` (a Telegram call), retrying transient failures with backoff. `what`
/// is a short label for the log line. Returns `op`'s last error if it never
/// succeeds. `op` is rebuilt each attempt (a fresh request per try).
pub async fn with_retry<F, Fut, T>(what: &str, mut op: F) -> Result<T, RequestError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, RequestError>>,
{
    let mut attempt: u32 = 0;
    loop {
        match op().await {
            Ok(value) => return Ok(value),
            Err(err) => {
                attempt += 1;
                let Some(delay) = backoff(&err, attempt) else {
                    // Non-transient, or attempts exhausted. Only worth a line if
                    // we actually retried; a first-try hard error is the
                    // caller's to log with its own context.
                    if attempt > 1 {
                        tracing::warn!(what, attempt, error = %err, "telegram call failed — giving up");
                    }
                    return Err(err);
                };
                tracing::warn!(
                    what,
                    attempt,
                    delay_ms = delay.as_millis() as u64,
                    error = %err,
                    "telegram call failed — backing off then retrying"
                );
                tokio::time::sleep(delay).await;
            }
        }
    }
}

/// The backoff delay for `err` on `attempt` (1-based), or `None` to stop (the
/// error is non-transient, or `attempt` reached [`MAX_ATTEMPTS`]).
fn backoff(err: &RequestError, attempt: u32) -> Option<Duration> {
    if attempt >= MAX_ATTEMPTS {
        return None;
    }
    match err {
        // Flood control: honour Telegram's requested wait (capped).
        RequestError::RetryAfter(secs) => Some(secs.duration().min(MAX_BACKOFF)),
        // Transient connectivity: 200ms, 400ms, 800ms, … capped.
        RequestError::Network(_) => {
            let shift = attempt.saturating_sub(1).min(10);
            Some((NETWORK_BASE * 2u32.pow(shift)).min(MAX_BACKOFF))
        }
        // A real API error, migrate, bad JSON, or I/O — not worth retrying.
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use teloxide::ApiError;
    use teloxide::types::Seconds;

    #[test]
    fn honours_the_servers_retry_after() {
        let err = RequestError::RetryAfter(Seconds::from_seconds(3));
        assert_eq!(backoff(&err, 1), Some(Duration::from_secs(3)));
    }

    #[test]
    fn caps_an_absurd_retry_after() {
        let err = RequestError::RetryAfter(Seconds::from_seconds(9999));
        assert_eq!(backoff(&err, 1), Some(MAX_BACKOFF));
    }

    #[test]
    fn gives_up_after_max_attempts() {
        let err = RequestError::RetryAfter(Seconds::from_seconds(1));
        assert!(backoff(&err, MAX_ATTEMPTS - 1).is_some());
        assert_eq!(
            backoff(&err, MAX_ATTEMPTS),
            None,
            "the final attempt must not schedule another retry"
        );
    }

    #[test]
    fn does_not_retry_a_plain_api_error() {
        let err = RequestError::Api(ApiError::Unknown("boom".to_string()));
        assert_eq!(backoff(&err, 1), None);
    }
}
