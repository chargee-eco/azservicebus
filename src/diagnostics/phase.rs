//! Payload-free timing of sender futures, including cancellation by an outer timeout.
use crate::util::time::Instant;

pub(crate) struct Phase {
    phase: &'static str,
    sender_id: u32,
    started: Instant,
    finished: bool,
}

impl Phase {
    pub(crate) fn new(phase: &'static str, sender_id: u32) -> Self {
        log::info!(target: "azservicebus::diagnostics",
            "phase={} sender_id={} outcome=start", phase, sender_id);
        Self {
            phase,
            sender_id,
            started: Instant::now(),
            finished: false,
        }
    }

    pub(crate) fn finish(mut self, outcome: &'static str) {
        self.finished = true;
        log::info!(target: "azservicebus::diagnostics",
            "phase={} sender_id={} elapsed_ms={} outcome={}",
            self.phase, self.sender_id, self.started.elapsed().as_millis(), outcome);
    }

    pub(crate) fn result<T, E>(self, result: Result<T, E>) -> Result<T, E> {
        if result.is_err() {
            log::warn!(target: "azservicebus::diagnostics",
                "phase={} sender_id={} error_type={}",
                self.phase, self.sender_id, std::any::type_name::<E>());
        }
        self.finish(if result.is_ok() { "ok" } else { "error" });
        result
    }
}

impl Drop for Phase {
    fn drop(&mut self) {
        if !self.finished {
            // Drop alone cannot distinguish an outer timeout from caller cancellation/panic.
            log::warn!(target: "azservicebus::diagnostics",
                "phase={} sender_id={} elapsed_ms={} outcome=dropped_pending",
                self.phase, self.sender_id, self.started.elapsed().as_millis());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use std::task::Poll;

    struct Capture(Mutex<Vec<String>>);
    static CAPTURE: Capture = Capture(Mutex::new(Vec::new()));
    impl log::Log for Capture {
        fn enabled(&self, metadata: &log::Metadata<'_>) -> bool {
            metadata.target() == "azservicebus::diagnostics"
        }
        fn log(&self, record: &log::Record<'_>) {
            if self.enabled(record.metadata()) {
                self.0.lock().unwrap().push(record.args().to_string());
            }
        }
        fn flush(&self) {}
    }

    #[tokio::test(start_paused = true)]
    async fn completion_error_and_cancelled_future_are_distinct() {
        log::set_logger(&CAPTURE).unwrap();
        log::set_max_level(log::LevelFilter::Info);
        assert_eq!(Phase::new("test_ok", 7).result::<_, ()>(Ok(42)), Ok(42));
        assert_eq!(
            Phase::new("test_error", 7).result::<(), _>(Err(42)),
            Err(42)
        );
        let mut pending = Box::pin(async {
            let phase = Phase::new("test_cancel", 7);
            std::future::pending::<()>().await;
            phase.finish("ok");
        });
        futures_util::future::poll_fn(|cx| {
            assert!(std::future::Future::poll(pending.as_mut(), cx).is_pending());
            Poll::Ready(())
        })
        .await;
        drop(pending);
        // Exercise the actual diagnostic timeout wrapper and nested receive timer.
        // These are isolated synthetic futures; no network or broker involved.
        use crate::diagnostics::receive::{timeout, ReceiveWait};
        use std::time::Duration;
        let outer = timeout(Duration::from_millis(1), async {
            let trace = ReceiveWait::new(99, 1, 1);
            trace.stage("amqp_recv");
            trace.sleep(Duration::from_secs(60)).await;
        })
        .await;
        assert!(outer.is_err());
        let empty = timeout(Duration::from_secs(60), async {
            let mut trace = ReceiveWait::new(100, 1, 1);
            trace.sleep(Duration::from_millis(1)).await;
            trace.finish("wait_elapsed");
            42
        })
        .await;
        assert_eq!(empty.unwrap(), 42);
        let failed = timeout(Duration::from_secs(60), async {
            let mut trace = ReceiveWait::new(101, 1, 1);
            trace.stage("remote_detached");
            trace.finish("amqp_error");
            Err::<(), _>("unchanged")
        })
        .await;
        assert_eq!(failed.unwrap(), Err("unchanged"));
        let mut cancelled = Box::pin(timeout(Duration::from_secs(60), async {
            let trace = ReceiveWait::new(102, 1, 1);
            trace.stage("amqp_recv");
            std::future::pending::<()>().await;
        }));
        futures_util::future::poll_fn(|cx| {
            assert!(std::future::Future::poll(cancelled.as_mut(), cx).is_pending());
            Poll::Ready(())
        })
        .await;
        drop(cancelled);
        // Equal deadlines: timer-kit polls the operation first, so the inner wait completes.
        let equal = timeout(Duration::from_secs(10), async {
            let mut trace = ReceiveWait::new(103, 1, 1);
            trace.sleep(Duration::from_secs(10)).await;
            trace.finish("wait_elapsed");
        })
        .await;
        assert!(equal.is_ok());
        // Same durations, but inner timer starts later: outer cancellation must be explicit.
        let delayed = timeout(Duration::from_secs(10), async {
            let trace = ReceiveWait::new(104, 1, 1);
            trace.stage("amqp_recv");
            tokio::time::advance(Duration::from_millis(1)).await;
            trace.sleep(Duration::from_secs(10)).await;
        })
        .await;
        assert!(delayed.is_err());
        let logs = CAPTURE.0.lock().unwrap();
        for (receiver, outcomes) in [
            (99, vec!["dropped_pending", "outer_timeout"]),
            (100, vec!["wait_elapsed", "outer_completed"]),
            (101, vec!["amqp_error", "outer_completed"]),
            (102, vec!["dropped_pending"]),
            (103, vec!["wait_elapsed", "outer_completed"]),
            (104, vec!["dropped_pending", "outer_timeout"]),
        ] {
            let lines: Vec<_> = logs
                .iter()
                .filter(|l| l.contains(&format!("receiver_id=Some({})", receiver)))
                .collect();
            for outcome in outcomes {
                assert!(
                    lines
                        .iter()
                        .any(|l| l.contains(&format!("outcome={} ", outcome))),
                    "missing {}",
                    outcome
                );
            }
            let poll_ids: std::collections::HashSet<_> = lines
                .iter()
                .map(|l| {
                    l.split_whitespace()
                        .find(|f| f.starts_with("poll_id="))
                        .unwrap()
                })
                .collect();
            assert_eq!(poll_ids.len(), 1);
            if receiver == 99 {
                let drop = lines
                    .iter()
                    .find(|l| l.contains("outcome=dropped_pending"))
                    .unwrap();
                assert!(drop.contains("stage=amqp_recv"));
                assert!(drop.contains("outer_due=true inner_due=false inner_elapsed=false"));
            }
            if receiver == 102 {
                assert!(!lines.iter().any(|l| l.contains("outcome=outer_timeout")));
            }
        }
        for (phase, outcome) in [
            ("test_ok", "ok"),
            ("test_error", "error"),
            ("test_cancel", "dropped_pending"),
        ] {
            let ended: Vec<_> = logs
                .iter()
                .filter(|line| {
                    line.contains(&format!("phase={} ", phase)) && line.contains("elapsed_ms=")
                })
                .collect();
            assert_eq!(ended.len(), 1);
            assert!(ended[0].ends_with(&format!("outcome={}", outcome)));
        }
    }
}
