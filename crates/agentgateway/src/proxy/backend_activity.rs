//! Transport-independent backend activity and background processor contracts.
//! Change notifications coalesce; absolute snapshots remain authoritative.

use futures::future::BoxFuture;
use serde::Serialize;
use tokio::task::JoinHandle;
use tracing::warn;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Binding {
	pub binding_uid: String,
	pub epoch: u64,
	pub draining: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BackendActivity {
	pub identity: String,
	pub outstanding: u64,
	pub binding: Binding,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Snapshot {
	pub process_generation: String,
	pub backends: Vec<BackendActivity>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct Change {
	pub urgent: bool,
}

pub(crate) trait Source: Send + Sync + 'static {
	/// Sample current state without network I/O, including configured zeros and live retired work.
	fn snapshot(&self) -> anyhow::Result<Snapshot>;

	/// One processor consumes these bounded wake hints; they are not a request-event log.
	fn changed(&self) -> BoxFuture<'_, Change>;
}

pub(crate) trait Processor: Send + Sync + 'static {
	/// Run until cancelled by the host. Transport retry and acknowledgement rules belong here.
	fn run<'a>(&'a self, source: &'a dyn Source) -> BoxFuture<'a, anyhow::Result<()>>;
}

pub(crate) fn spawn(
	processor: impl Processor,
	source: impl Source,
	drain: agent_core::drain::DrainWatcher,
) -> JoinHandle<anyhow::Result<()>> {
	tokio::spawn(async move {
		tokio::select! {
			_ = drain.wait_for_drain() => Ok(()),
			result = processor.run(&source) => {
				let error = match result {
					Ok(()) => anyhow::anyhow!("backend activity processor exited before shutdown"),
					Err(error) => error.context("backend activity processor failed"),
				};
				warn!(target: "activation", %error, phase = "processor_failed", "backend activity processor stopped");
				Err(error)
			},
		}
	})
}

#[cfg(test)]
mod tests {
	use std::sync::atomic::{AtomicBool, Ordering};
	use std::sync::{Arc, Mutex};
	use std::time::Duration;

	use anyhow::Context;
	use tokio::sync::{Notify, mpsc};

	use super::*;

	#[derive(Clone)]
	struct TestSource(Arc<SharedSource>);

	struct SharedSource {
		snapshot: Mutex<Snapshot>,
		changes: Notify,
		urgent: AtomicBool,
		unavailable: AtomicBool,
	}

	impl TestSource {
		fn new(outstanding: u64) -> Self {
			Self(Arc::new(SharedSource {
				snapshot: Mutex::new(Snapshot {
					process_generation: "process-1".into(),
					backends: vec![BackendActivity {
						identity: "ns/service-uid/80".into(),
						outstanding,
						binding: Binding {
							binding_uid: "binding-uid".into(),
							epoch: 2,
							draining: false,
						},
					}],
				}),
				changes: Notify::new(),
				urgent: AtomicBool::new(false),
				unavailable: AtomicBool::new(false),
			}))
		}

		fn set_count(&self, count: u64) {
			let mut snapshot = self.0.snapshot.lock().unwrap();
			let backend = &mut snapshot.backends[0];
			if backend.outstanding == 0 && count > 0 {
				self.0.urgent.store(true, Ordering::Release);
			}
			backend.outstanding = count;
			drop(snapshot);
			self.0.changes.notify_one();
		}
	}

	impl Source for TestSource {
		fn snapshot(&self) -> anyhow::Result<Snapshot> {
			anyhow::ensure!(
				!self.0.unavailable.load(Ordering::Acquire),
				"activity source unavailable"
			);
			Ok(self.0.snapshot.lock().unwrap().clone())
		}

		fn changed(&self) -> BoxFuture<'_, Change> {
			Box::pin(async move {
				self.0.changes.notified().await;
				Change {
					urgent: self.0.urgent.swap(false, Ordering::AcqRel),
				}
			})
		}
	}

	type Observation = (Snapshot, Option<Change>);

	struct RecordingProcessor(mpsc::Sender<Observation>);

	impl Processor for RecordingProcessor {
		fn run<'a>(&'a self, source: &'a dyn Source) -> BoxFuture<'a, anyhow::Result<()>> {
			Box::pin(async move {
				self
					.0
					.send((source.snapshot()?, None))
					.await
					.context("record initial activity")?;
				loop {
					let change = source.changed().await;
					self
						.0
						.send((source.snapshot()?, Some(change)))
						.await
						.context("record changed activity")?;
				}
			})
		}
	}

	async fn receive(rx: &mut mpsc::Receiver<Observation>) -> Observation {
		tokio::time::timeout(Duration::from_secs(3), rx.recv())
			.await
			.unwrap()
			.unwrap()
	}

	#[tokio::test]
	async fn recording_processor_observes_initial_state_and_coalesced_latest_demand() {
		let source = TestSource::new(0);
		let (tx, mut rx) = mpsc::channel(1);
		let (trigger, drain) = agent_core::drain::new();
		let task = spawn(RecordingProcessor(tx), source.clone(), drain);
		let (initial, change) = receive(&mut rx).await;
		assert_eq!(initial, source.snapshot().unwrap());
		assert!(change.is_none());
		for count in 1..=10_000 {
			source.set_count(count);
		}
		let (latest, change) = receive(&mut rx).await;
		assert_eq!(latest, source.snapshot().unwrap());
		assert_eq!(latest.backends[0].outstanding, 10_000);
		assert!(change.unwrap().urgent);
		tokio::time::timeout(
			Duration::from_secs(3),
			trigger.start_drain_and_wait(agent_core::drain::DrainMode::Immediate),
		)
		.await
		.unwrap();
		task.await.unwrap().unwrap();
	}

	struct StoppedProcessor {
		fail: bool,
	}

	impl Processor for StoppedProcessor {
		fn run<'a>(&'a self, _source: &'a dyn Source) -> BoxFuture<'a, anyhow::Result<()>> {
			Box::pin(async move {
				anyhow::ensure!(!self.fail, "processor unavailable");
				Ok(())
			})
		}
	}

	#[tokio::test]
	async fn processor_failure_and_unexpected_exit_are_errors_without_resetting_demand() {
		for fail in [true, false] {
			let source = TestSource::new(7);
			let (_trigger, drain) = agent_core::drain::new();
			let error = spawn(StoppedProcessor { fail }, source.clone(), drain)
				.await
				.unwrap()
				.unwrap_err();
			assert!(format!("{error:#}").contains(if fail {
				"processor unavailable"
			} else {
				"exited before shutdown"
			}));
			assert_eq!(source.snapshot().unwrap().backends[0].outstanding, 7);
		}
	}

	#[tokio::test]
	async fn source_failure_is_not_exported_as_an_empty_or_zero_snapshot() {
		let source = TestSource::new(7);
		let (tx, mut rx) = mpsc::channel(1);
		let (_trigger, drain) = agent_core::drain::new();
		let task = spawn(RecordingProcessor(tx), source.clone(), drain);
		assert_eq!(receive(&mut rx).await.0.backends[0].outstanding, 7);
		source.0.unavailable.store(true, Ordering::Release);
		source.0.changes.notify_one();
		let error = task.await.unwrap().unwrap_err();
		assert!(format!("{error:#}").contains("activity source unavailable"));
		assert!(rx.recv().await.is_none());
		assert_eq!(source.0.snapshot.lock().unwrap().backends[0].outstanding, 7);
	}

	struct DropSignal(Arc<AtomicBool>);

	impl Drop for DropSignal {
		fn drop(&mut self) {
			self.0.store(true, Ordering::Release);
		}
	}

	struct BlockedProcessor {
		started: Arc<Notify>,
		dropped: Arc<AtomicBool>,
	}

	impl Processor for BlockedProcessor {
		fn run<'a>(&'a self, _source: &'a dyn Source) -> BoxFuture<'a, anyhow::Result<()>> {
			Box::pin(async move {
				let _guard = DropSignal(self.dropped.clone());
				self.started.notify_one();
				std::future::pending().await
			})
		}
	}

	#[tokio::test]
	async fn shutdown_cancels_blocked_processing() {
		let started = Arc::new(Notify::new());
		let dropped = Arc::new(AtomicBool::new(false));
		let (trigger, drain) = agent_core::drain::new();
		let task = spawn(
			BlockedProcessor {
				started: started.clone(),
				dropped: dropped.clone(),
			},
			TestSource::new(7),
			drain,
		);
		tokio::time::timeout(Duration::from_secs(3), started.notified())
			.await
			.unwrap();
		tokio::time::timeout(
			Duration::from_secs(3),
			trigger.start_drain_and_wait(agent_core::drain::DrainMode::Immediate),
		)
		.await
		.unwrap();
		task.await.unwrap().unwrap();
		assert!(dropped.load(Ordering::Acquire));
	}
}
