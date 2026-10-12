//! Receive-only diagnostics. Never records message bodies, tags, URLs or peer descriptions.
use crate::util::time::{Delay, Instant};
use std::{
    future::Future,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

static NEXT_ID: AtomicU64 = AtomicU64::new(1);
tokio::task_local! { static ATTEMPT: Arc<Mutex<Attempt>>; }
struct Attempt {
    id: u64,
    started: Instant,
    outer_deadline: Instant,
    inner_deadline: Option<Instant>,
    receiver: Option<u32>,
    phase: &'static str,
    inner_elapsed: bool,
    buffered: usize,
    prefetch: u32,
    max_messages: u32,
}
impl Attempt {
    fn log(&self, outcome: &'static str) {
        let now = Instant::now();
        let inner_us = self
            .inner_deadline
            .map(|d| d.duration_since(self.started).as_micros());
        log::info!(target: "azservicebus::diagnostics",
            "phase=receive_wait poll_id={} receiver_id={:?} stage={} outcome={} elapsed_us={} outer_deadline_us={} inner_deadline_us={:?} outer_due={} inner_due={} inner_elapsed={} buffered={} prefetch={} max_messages={}",
            self.id, self.receiver, self.phase, outcome, now.duration_since(self.started).as_micros(),
            self.outer_deadline.duration_since(self.started).as_micros(), inner_us,
            now >= self.outer_deadline, self.inner_deadline.is_some_and(|d| now >= d), self.inner_elapsed, self.buffered, self.prefetch, self.max_messages);
    }
}

/// Same timer-kit timeout and poll ordering; task-local data only correlates nested diagnostics.
pub(crate) async fn timeout<F: Future>(
    duration: Duration,
    future: F,
) -> Result<F::Output, timer_kit::error::Elapsed> {
    let started = Instant::now();
    let deadline = started + duration;
    let attempt = Arc::new(Mutex::new(Attempt {
        id: NEXT_ID.fetch_add(1, Ordering::Relaxed),
        started,
        outer_deadline: deadline,
        inner_deadline: None,
        receiver: None,
        phase: "not_started",
        inner_elapsed: false,
        buffered: 0,
        prefetch: 0,
        max_messages: 0,
    }));
    let result = ATTEMPT
        .scope(
            attempt.clone(),
            timer_kit::timeout_at::<Delay, _>(deadline, future),
        )
        .await;
    let state = attempt.lock().unwrap();
    if state.receiver.is_some() {
        state.log(if result.is_err() {
            "outer_timeout"
        } else {
            "outer_completed"
        });
    }
    result
}

pub(crate) struct ReceiveWait {
    attempt: Option<Arc<Mutex<Attempt>>>,
    finished: bool,
    scope: Option<Arc<tokio::sync::Mutex<crate::amqp::amqp_connection_scope::AmqpConnectionScope>>>,
}
impl ReceiveWait {
    pub(crate) fn new(receiver: u32, prefetch: u32, max_messages: u32) -> Self {
        let attempt = ATTEMPT.try_with(Arc::clone).ok();
        if let Some(a) = &attempt {
            let mut s = a.lock().unwrap();
            s.receiver = Some(receiver);
            s.phase = "select";
            s.prefetch = prefetch;
            s.max_messages = max_messages;
        }
        Self {
            attempt,
            finished: false,
            scope: None,
        }
    }
    pub(crate) fn observe_scope(
        &mut self,
        scope: Arc<tokio::sync::Mutex<crate::amqp::amqp_connection_scope::AmqpConnectionScope>>,
    ) {
        self.scope = Some(scope);
    }
    fn log_scope(&self) {
        if let Some(a) = &self.attempt {
            let id = a.lock().unwrap().id;
            let state = self
                .scope
                .as_ref()
                .and_then(|s| s.try_lock().ok().map(|s| s.diagnostic_state()));
            // Never wait for the recovery mutex from diagnostics or Drop.
            log::info!(target: "azservicebus::diagnostics",
                "phase=receive_transport poll_id={} connection_closed={:?} session_ended={:?} scope_disposed={:?}", id, state.map(|s|s.0), state.map(|s|s.1), state.map(|s|s.2));
        }
    }
    pub(crate) fn stage(&self, phase: &'static str) {
        if let Some(a) = &self.attempt {
            let mut s = a.lock().unwrap();
            s.phase = phase;
        }
    }
    pub(crate) async fn sleep(&self, duration: Duration) {
        // Construct the same timer here and record its own deadline (not an estimate from a log timestamp).
        let sleep = timer_kit::sleep::<Delay>(duration);
        if let Some(a) = &self.attempt {
            let mut s = a.lock().unwrap();
            s.inner_deadline = Some(sleep.deadline());
        }
        let _ = sleep.await;
        if let Some(a) = &self.attempt {
            let mut s = a.lock().unwrap();
            s.inner_elapsed = true;
        }
    }
    pub(crate) fn buffered(&self, count: usize) {
        if let Some(a) = &self.attempt {
            a.lock().unwrap().buffered = count;
        }
    }
    pub(crate) fn finish(&mut self, outcome: &'static str) {
        self.finished = true;
        if let Some(a) = &self.attempt {
            a.lock().unwrap().log(outcome);
        }
        self.log_scope();
    }
}
impl Drop for ReceiveWait {
    fn drop(&mut self) {
        if !self.finished {
            // Drop alone is cancellation, not proof of a timeout; correlate with outer_timeout.
            if let Some(a) = &self.attempt {
                a.lock().unwrap().log("dropped_pending");
            }
            self.log_scope();
        }
    }
}
