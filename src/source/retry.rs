//! Generic exponential-backoff retry wrapper (KS-03: "retry failed
//! object-ID batches with exponential backoff"). Not KGS-specific — any
//! future HTTP-based importer (KCC operators, intents, ...) should reuse
//! this rather than hand-rolling its own retry loop.

use std::future::Future;
use std::time::Duration;

#[derive(Debug, Clone, Copy)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub base_delay: Duration,
}

impl Default for RetryPolicy {
    /// 5 attempts, doubling from 500ms: 500ms, 1s, 2s, 4s between
    /// attempts 1-5 (4 waits total after the first try).
    fn default() -> Self {
        Self {
            max_attempts: 5,
            base_delay: Duration::from_millis(500),
        }
    }
}

impl RetryPolicy {
    pub fn delay_for_attempt(&self, attempt: u32) -> Duration {
        self.base_delay * 2u32.saturating_pow(attempt.saturating_sub(1))
    }
}

/// Calls `f` up to `policy.max_attempts` times, sleeping with exponential
/// backoff between attempts, returning the first success or the final
/// error. `f` is called fresh each attempt (an `FnMut` producing a new
/// future), since a `reqwest` request can't be retried by re-polling the
/// same future.
pub async fn with_retry<T, E, F, Fut>(policy: RetryPolicy, mut f: F) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
{
    let mut attempt = 0u32;
    loop {
        attempt += 1;
        match f().await {
            Ok(value) => return Ok(value),
            Err(err) if attempt >= policy.max_attempts => return Err(err),
            Err(_) => {
                tokio::time::sleep(policy.delay_for_attempt(attempt)).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    #[test]
    fn delay_doubles_each_attempt() {
        let policy = RetryPolicy {
            max_attempts: 5,
            base_delay: Duration::from_millis(100),
        };
        assert_eq!(policy.delay_for_attempt(1), Duration::from_millis(100));
        assert_eq!(policy.delay_for_attempt(2), Duration::from_millis(200));
        assert_eq!(policy.delay_for_attempt(3), Duration::from_millis(400));
        assert_eq!(policy.delay_for_attempt(4), Duration::from_millis(800));
    }

    #[tokio::test]
    async fn succeeds_without_retrying_on_first_success() {
        let calls = AtomicU32::new(0);
        let policy = RetryPolicy {
            max_attempts: 3,
            base_delay: Duration::from_millis(1),
        };
        let result: Result<u32, &str> = with_retry(policy, || async {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok(42)
        })
        .await;
        assert_eq!(result, Ok(42));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn retries_until_success_within_max_attempts() {
        let calls = AtomicU32::new(0);
        let policy = RetryPolicy {
            max_attempts: 5,
            base_delay: Duration::from_millis(1),
        };
        let result: Result<u32, &str> = with_retry(policy, || {
            let n = calls.fetch_add(1, Ordering::SeqCst) + 1;
            async move {
                if n < 3 {
                    Err("not yet")
                } else {
                    Ok(99)
                }
            }
        })
        .await;
        assert_eq!(result, Ok(99));
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn gives_up_after_max_attempts() {
        let calls = AtomicU32::new(0);
        let policy = RetryPolicy {
            max_attempts: 3,
            base_delay: Duration::from_millis(1),
        };
        let result: Result<u32, &str> = with_retry(policy, || {
            calls.fetch_add(1, Ordering::SeqCst);
            async { Err("always fails") }
        })
        .await;
        assert_eq!(result, Err("always fails"));
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }
}
