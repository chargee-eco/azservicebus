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

    #[tokio::test]
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
        let logs = CAPTURE.0.lock().unwrap();
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
