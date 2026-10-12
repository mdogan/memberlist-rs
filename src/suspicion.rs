// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0
//
// This file is a Rust port of suspicion.go from https://github.com/hashicorp/memberlist.

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicI32, Ordering};
use std::time::{Duration, Instant};

use crate::timer::Timer;

/// Manages the suspect timer for a node and provides an interface to
/// accelerate the timeout as we get more independent confirmations that a
/// node is suspect.
pub(crate) struct Suspicion {
    /// The number of independent confirmations we've seen. This must be
    /// updated using atomic instructions to prevent contention with the
    /// timer callback.
    n: Arc<AtomicI32>,

    /// The number of independent confirmations we'd like to see in order to
    /// drive the timer to its minimum value.
    k: i32,

    /// The minimum timer value.
    min: Duration,

    /// The maximum timer value.
    max: Duration,

    /// Captures the timestamp when we began the timer. This is used so we
    /// can calculate durations to feed the timer during updates in a way
    /// that achieves the overall time we'd like.
    start: Instant,

    /// The underlying timer that implements the timeout.
    timer: Timer,

    /// The function to call when the timer expires. We hold on to this
    /// because there are cases where we call it directly.
    timeout_fn: Arc<dyn Fn() + Send + Sync>,

    /// The "from" nodes that have confirmed a given node is suspect. This
    /// prevents double counting.
    confirmations: HashSet<String>,
}

impl Suspicion {
    /// Returns a timer started with the max time, and that will drive to the
    /// min time after seeing k or more confirmations. The from node will be
    /// excluded from confirmations since we might get our own suspicion
    /// message gossiped back to us. The minimum time will be used if no
    /// confirmations are called for (k <= 0).
    pub(crate) fn new(
        from: &str,
        k: i32,
        min: Duration,
        max: Duration,
        f: impl Fn(i32) + Send + Sync + 'static,
    ) -> Suspicion {
        // Exclude the from node from any confirmations.
        let mut confirmations = HashSet::new();
        confirmations.insert(from.to_string());

        // Pass the number of confirmations into the timeout function for
        // easy telemetry.
        let n = Arc::new(AtomicI32::new(0));
        let timeout_fn: Arc<dyn Fn() + Send + Sync> = {
            let n = n.clone();
            Arc::new(move || f(n.load(Ordering::SeqCst)))
        };

        // If there aren't any confirmations to be made then take the min
        // time from the start.
        let timeout = if k < 1 { min } else { max };
        let timer = {
            let timeout_fn = timeout_fn.clone();
            Timer::after_func(timeout, move || timeout_fn())
        };

        // Capture the start time right after starting the timer above so
        // we should always err on the side of a little longer timeout if
        // there's any preemption that separates this and the step above.
        Suspicion {
            n,
            k,
            min,
            max,
            start: Instant::now(),
            timer,
            timeout_fn,
            confirmations,
        }
    }

    /// Registers that a possibly new peer has also determined the given node
    /// is suspect. This returns true if this was new information, and false
    /// if it was a duplicate confirmation, or if we've got enough
    /// confirmations to hit the minimum.
    pub(crate) fn confirm(&mut self, from: &str) -> bool {
        // If we've got enough confirmations then stop accepting them.
        if self.n.load(Ordering::SeqCst) >= self.k {
            return false;
        }

        // Only allow one confirmation from each possible peer.
        if !self.confirmations.insert(from.to_string()) {
            return false;
        }

        // Compute the new timeout given the current number of confirmations
        // and adjust the timer. If the timeout becomes negative *and* we can
        // cleanly stop the timer then we will call the timeout function
        // directly from here.
        let n = self.n.fetch_add(1, Ordering::SeqCst) + 1;
        let elapsed = self.start.elapsed();
        let remaining = remaining_suspicion_time(n, self.k, elapsed, self.min, self.max);
        if self.timer.stop() {
            if remaining > 0 {
                self.timer.reset(Duration::from_nanos(remaining as u64));
            } else {
                let timeout_fn = self.timeout_fn.clone();
                tokio::spawn(async move { timeout_fn() });
            }
        }
        true
    }
}

/// Takes the state variables of the suspicion timer and calculates the
/// remaining time to wait before considering a node dead. The result is in
/// nanoseconds and can be negative, so be prepared to fire the timer
/// immediately in that case.
pub(crate) fn remaining_suspicion_time(
    n: i32,
    k: i32,
    elapsed: Duration,
    min: Duration,
    max: Duration,
) -> i64 {
    let frac = (n as f64 + 1.0).ln() / (k as f64 + 1.0).ln();
    let raw = max.as_secs_f64() - frac * (max.as_secs_f64() - min.as_secs_f64());
    let mut timeout = (1000.0 * raw).floor() as i64 * 1_000_000;
    if timeout < min.as_nanos() as i64 {
        timeout = min.as_nanos() as i64;
    }

    // We have to take into account the amount of time that has passed so
    // far, so we get the right overall timeout.
    timeout - elapsed.as_nanos() as i64
}

#[cfg(test)]
mod tests {
    use tokio::sync::mpsc;
    use tokio::time::sleep;

    use super::*;

    const SEC: i64 = 1_000_000_000;
    const MS: i64 = 1_000_000;

    #[test]
    fn test_suspicion_remaining_suspicion_time() {
        // (n, k, elapsed, min, max, expected)
        let cases: [(i32, i32, u64, u64, u64, i64); 6] = [
            (0, 3, 0, 2, 30, 30 * SEC),
            (1, 3, 2, 2, 30, 14 * SEC),
            (2, 3, 3, 2, 30, 4810 * MS),
            (3, 3, 4, 2, 30, -2 * SEC),
            (4, 3, 5, 2, 30, -3 * SEC),
            (5, 3, 10, 2, 30, -8 * SEC),
        ];
        for (i, (n, k, elapsed, min, max, expected)) in cases.into_iter().enumerate() {
            let remaining = remaining_suspicion_time(
                n,
                k,
                Duration::from_secs(elapsed),
                Duration::from_secs(min),
                Duration::from_secs(max),
            );
            assert_eq!(remaining, expected, "case {i}");
        }
    }

    #[tokio::test]
    async fn test_suspicion_timer() {
        const K: i32 = 3;
        const MIN: Duration = Duration::from_millis(500);
        const MAX: Duration = Duration::from_secs(2);

        struct Case {
            num_confirmations: i32,
            from: &'static str,
            confirmations: Vec<(&'static str, bool)>,
            expected: Duration,
        }
        let cases = vec![
            Case {
                num_confirmations: 0,
                from: "me",
                confirmations: vec![],
                expected: MAX,
            },
            Case {
                num_confirmations: 1,
                from: "me",
                confirmations: vec![("me", false), ("foo", true)],
                expected: Duration::from_millis(1250),
            },
            Case {
                num_confirmations: 1,
                from: "me",
                confirmations: vec![("me", false), ("foo", true), ("foo", false), ("foo", false)],
                expected: Duration::from_millis(1250),
            },
            Case {
                num_confirmations: 2,
                from: "me",
                confirmations: vec![("me", false), ("foo", true), ("bar", true)],
                expected: Duration::from_millis(810),
            },
            Case {
                num_confirmations: 3,
                from: "me",
                confirmations: vec![("me", false), ("foo", true), ("bar", true), ("baz", true)],
                expected: MIN,
            },
            Case {
                num_confirmations: 3,
                from: "me",
                confirmations: vec![
                    ("me", false),
                    ("foo", true),
                    ("bar", true),
                    ("baz", true),
                    ("zoo", false),
                ],
                expected: MIN,
            },
        ];
        for (i, c) in cases.into_iter().enumerate() {
            let (tx, mut rx) = mpsc::unbounded_channel::<(Duration, i32)>();
            let start = Instant::now();
            let f = move |num_confirmations: i32| {
                let _ = tx.send((start.elapsed(), num_confirmations));
            };

            // Create the timer and add the requested confirmations. Wait
            // the fudge amount to help make sure we calculate the timeout
            // overall, and don't accumulate extra time.
            let mut s = Suspicion::new(c.from, K, MIN, MAX, f);
            let fudge = Duration::from_millis(25);
            for (from, new_info) in &c.confirmations {
                sleep(fudge).await;
                assert_eq!(
                    s.confirm(from),
                    *new_info,
                    "case {i}: newInfo mismatch for {from}"
                );
            }

            // Wait until right before the timeout and make sure the
            // timer hasn't fired.
            let already = fudge * c.confirmations.len() as u32;
            sleep(c.expected - already - fudge).await;
            if let Ok((d, _)) = rx.try_recv() {
                panic!("case {i}: should not have fired ({:9.6})", d.as_secs_f64());
            }

            // Wait through the timeout and a little after and make sure it
            // fires.
            sleep(2 * fudge).await;
            match rx.try_recv() {
                Ok((_, num_confirmations)) => {
                    assert_eq!(
                        num_confirmations, c.num_confirmations,
                        "case {i}: bad confirmations"
                    )
                }
                Err(_) => panic!("case {i}: should have fired"),
            }

            // Confirm after to make sure it handles a negative remaining
            // time correctly and doesn't fire again.
            s.confirm("late");
            sleep(c.expected + 2 * fudge).await;
            if let Ok((d, _)) = rx.try_recv() {
                panic!("case {i}: should not have fired ({:9.6})", d.as_secs_f64());
            }
        }
    }

    #[tokio::test]
    async fn test_suspicion_timer_zero_k() {
        let (tx, mut rx) = mpsc::unbounded_channel::<()>();
        let f = move |_: i32| {
            let _ = tx.send(());
        };

        // This should select the min time since there are no expected
        // confirmations to accelerate the timer.
        let mut s = Suspicion::new(
            "me",
            0,
            Duration::from_millis(25),
            Duration::from_secs(30),
            f,
        );
        assert!(!s.confirm("foo"), "should not provide new information");

        tokio::time::timeout(Duration::from_millis(50), rx.recv())
            .await
            .expect("should have fired");
    }

    #[tokio::test]
    async fn test_suspicion_timer_immediate() {
        let (tx, mut rx) = mpsc::unbounded_channel::<()>();
        let f = move |_: i32| {
            let _ = tx.send(());
        };

        // This should underflow the timeout and fire immediately.
        let mut s = Suspicion::new(
            "me",
            1,
            Duration::from_millis(100),
            Duration::from_secs(30),
            f,
        );
        sleep(Duration::from_millis(200)).await;
        s.confirm("foo");

        // Wait a little while since the function gets called in a task.
        tokio::time::timeout(Duration::from_millis(25), rx.recv())
            .await
            .expect("should have fired");
    }
}
