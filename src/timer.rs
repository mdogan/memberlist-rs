// Copyright (c) 2026 Mehmet Dogan
// SPDX-License-Identifier: MPL-2.0

use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use tokio::task::JoinHandle;

/// A timer that calls a function after a delay, like Go's `time.AfterFunc`.
///
/// The function runs on a tokio task, so a timer can only be created inside
/// a tokio runtime. Dropping the timer stops it.
pub(crate) struct Timer {
    inner: Arc<Inner>,
}

struct Inner {
    f: Box<dyn Fn() + Send + Sync>,
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    /// Incremented each time the timer is armed, so that an old task can see
    /// that it has been replaced.
    generation: u64,
    /// True while the timer is armed and has not fired yet.
    pending: bool,
    task: Option<JoinHandle<()>>,
}

impl Timer {
    /// Waits for the duration to elapse and then calls `f` in its own task.
    pub(crate) fn after_func(d: Duration, f: impl Fn() + Send + Sync + 'static) -> Timer {
        let timer = Timer {
            inner: Arc::new(Inner {
                f: Box::new(f),
                state: Mutex::new(State::default()),
            }),
        };
        timer.arm(d);
        timer
    }

    fn arm(&self, d: Duration) {
        let mut state = self.inner.state.lock();
        state.generation += 1;
        state.pending = true;
        let generation = state.generation;
        let inner = self.inner.clone();
        state.task = Some(tokio::spawn(async move {
            tokio::time::sleep(d).await;
            {
                let mut state = inner.state.lock();
                if !state.pending || state.generation != generation {
                    return;
                }
                state.pending = false;
            }
            (inner.f)();
        }));
    }

    /// Prevents the timer from firing. It returns true if the call stops the
    /// timer, false if the timer has already expired or been stopped.
    pub(crate) fn stop(&self) -> bool {
        let mut state = self.inner.state.lock();
        if !state.pending {
            return false;
        }
        state.pending = false;
        if let Some(task) = state.task.take() {
            task.abort();
        }
        true
    }

    /// Changes the timer to expire after duration `d`. This must only be
    /// called on a timer that is stopped or has expired.
    pub(crate) fn reset(&self, d: Duration) {
        self.arm(d);
    }
}

impl Drop for Timer {
    fn drop(&mut self) {
        self.stop();
    }
}
