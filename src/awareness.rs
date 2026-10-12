// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0
//
// This file is a Rust port of awareness.go from https://github.com/hashicorp/memberlist.

use std::time::Duration;

use metrics::Label;
use parking_lot::RwLock;

/// Manages a simple metric for tracking the estimated health of the local
/// node. Health is primary the node's ability to respond in the soft
/// real-time manner required for correct health checking of other nodes in
/// the cluster.
pub(crate) struct Awareness {
    /// The upper threshold for the timeout scale (the score will be
    /// constrained to be from 0 <= score < max).
    max: i64,

    /// The current awareness score. Lower values are healthier and zero is
    /// the minimum value.
    score: RwLock<i64>,

    /// The labels to put on all emitted metrics.
    metric_labels: Vec<Label>,
}

impl Awareness {
    /// Returns a new awareness object.
    pub(crate) fn new(max: usize, metric_labels: Vec<Label>) -> Self {
        Awareness {
            max: max as i64,
            score: RwLock::new(0),
            metric_labels,
        }
    }

    /// Takes the given delta and applies it to the score in a thread-safe
    /// manner. It also enforces a floor of zero and a max of max, so deltas
    /// may not change the overall score if it's railed at one of the
    /// extremes.
    pub(crate) fn apply_delta(&self, delta: i64) {
        let (initial, fin) = {
            let mut score = self.score.write();
            let initial = *score;
            *score += delta;
            if *score < 0 {
                *score = 0;
            } else if *score > self.max - 1 {
                *score = self.max - 1;
            }
            (initial, *score)
        };

        if initial != fin {
            metrics::gauge!("memberlist.health.score", self.metric_labels.clone()).set(fin as f64);
        }
    }

    /// Returns the raw health score.
    pub(crate) fn get_health_score(&self) -> i64 {
        *self.score.read()
    }

    /// Takes the given duration and scales it based on the current score.
    /// Less healthyness will lead to longer timeouts.
    pub(crate) fn scale_timeout(&self, timeout: Duration) -> Duration {
        let score = *self.score.read();
        timeout * (score + 1).max(0) as u32
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_awareness() {
        // (delta, score, timeout)
        let cases: [(i64, i64, Duration); 14] = [
            (0, 0, Duration::from_secs(1)),
            (-1, 0, Duration::from_secs(1)),
            (-10, 0, Duration::from_secs(1)),
            (1, 1, Duration::from_secs(2)),
            (-1, 0, Duration::from_secs(1)),
            (10, 7, Duration::from_secs(8)),
            (-1, 6, Duration::from_secs(7)),
            (-1, 5, Duration::from_secs(6)),
            (-1, 4, Duration::from_secs(5)),
            (-1, 3, Duration::from_secs(4)),
            (-1, 2, Duration::from_secs(3)),
            (-1, 1, Duration::from_secs(2)),
            (-1, 0, Duration::from_secs(1)),
            (-1, 0, Duration::from_secs(1)),
        ];

        let a = Awareness::new(8, Vec::new());
        for (i, (delta, score, timeout)) in cases.into_iter().enumerate() {
            a.apply_delta(delta);
            assert_eq!(a.get_health_score(), score, "case {i}: score mismatch");
            assert_eq!(
                a.scale_timeout(Duration::from_secs(1)),
                timeout,
                "case {i}: scaled timeout mismatch"
            );
        }
    }
}
