use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, ensure};
use futures::future::BoxFuture;
use protos::activation::demand_publisher_client::DemandPublisherClient;
use protos::activation::{BackendDemand, DemandAck, DemandSnapshot};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Endpoint;
use tracing::{debug, warn};

use super::backend_activity::{Processor, Source};

const HEARTBEAT: Duration = Duration::from_millis(500);
const COALESCE: Duration = Duration::from_millis(20);
const ACK_TIMEOUT: Duration = Duration::from_secs(2);

pub(crate) struct GrpcDemandProcessor {
	endpoint: Endpoint,
	pod_uid: String,
}

impl GrpcDemandProcessor {
	pub(crate) fn from_env() -> anyhow::Result<Option<Self>> {
		let address = match std::env::var("ACTIVATION_ADAPTER_ADDRESS") {
			Ok(address) => address,
			Err(std::env::VarError::NotPresent) => return Ok(None),
			Err(error) => return Err(error).context("invalid ACTIVATION_ADAPTER_ADDRESS"),
		};
		ensure!(
			address.starts_with("http://"),
			"ACTIVATION_ADAPTER_ADDRESS requires an explicit http:// local experiment endpoint"
		);
		let endpoint = Endpoint::from_shared(address)?.connect_timeout(Duration::from_secs(1));
		let pod_uid = std::env::var("POD_UID").context("POD_UID is required for demand push")?;
		ensure!(!pod_uid.is_empty(), "POD_UID must not be empty");
		Ok(Some(Self { endpoint, pod_uid }))
	}
}

impl Processor for GrpcDemandProcessor {
	fn run<'a>(&'a self, source: &'a dyn Source) -> BoxFuture<'a, anyhow::Result<()>> {
		Box::pin(async move {
			let mut backoff = Duration::from_millis(100);
			loop {
				let started = tokio::time::Instant::now();
				if let Err(error) = session(&self.endpoint, source, &self.pod_uid).await {
					warn!(target: "activation", %error, phase = "publisher_disconnected", "demand stream unavailable");
				}
				if started.elapsed() > Duration::from_secs(5) {
					backoff = Duration::from_millis(100);
				}
				tokio::time::sleep(backoff + Duration::from_millis(rand::random_range(0..=50))).await;
				backoff = (backoff * 2).min(Duration::from_secs(2));
			}
		})
	}
}

fn frame(
	source: &dyn Source,
	pod_uid: &str,
	sequence: u64,
	session_id: &str,
) -> anyhow::Result<DemandSnapshot> {
	let snapshot = source.snapshot()?;
	let backends = snapshot
		.backends
		.into_iter()
		.map(|backend| {
			Ok(BackendDemand {
				identity: backend.identity,
				outstanding: i64::try_from(backend.outstanding)
					.context("outstanding demand exceeds int64")?,
				binding_uid: backend.binding.binding_uid,
				epoch: backend.binding.epoch,
				draining: backend.binding.draining,
			})
		})
		.collect::<anyhow::Result<Vec<_>>>()?;
	Ok(DemandSnapshot {
		protocol_version: 1,
		pod_uid: pod_uid.to_owned(),
		process_generation: snapshot.process_generation,
		sequence,
		sampled_at_unix_millis: i64::try_from(
			SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis(),
		)
		.context("sample timestamp exceeds int64")?,
		backends,
		session_id: session_id.to_owned(),
	})
}

fn validate_ack(ack: &DemandAck, sequence: u64, session_id: &str) -> anyhow::Result<()> {
	ensure!(
		!ack.session_id.is_empty(),
		"adapter returned an empty session ID"
	);
	ensure!(
		ack.sequence == sequence,
		"adapter acknowledgement sequence mismatch"
	);
	ensure!(
		session_id.is_empty() || ack.session_id == session_id,
		"adapter session changed"
	);
	Ok(())
}

async fn session(endpoint: &Endpoint, source: &dyn Source, pod_uid: &str) -> anyhow::Result<()> {
	let channel = endpoint.connect().await.context("connect demand adapter")?;
	let mut client = DemandPublisherClient::new(channel)
		.max_encoding_message_size(1 << 20)
		.max_decoding_message_size(1 << 20);
	let (tx, rx) = mpsc::channel(1);
	let mut sequence = 1;
	tx.send(frame(source, pod_uid, sequence, "")?)
		.await
		.context("initial demand frame")?;
	let mut responses = tokio::time::timeout(ACK_TIMEOUT, client.publish(ReceiverStream::new(rx)))
		.await
		.context("demand registration timed out")??
		.into_inner();
	let mut session_id = String::new();
	let mut heartbeat = tokio::time::interval(HEARTBEAT);
	heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
	heartbeat.tick().await;
	loop {
		let ack = tokio::time::timeout(ACK_TIMEOUT, responses.message())
			.await
			.context("demand acknowledgement timed out")??
			.context("demand acknowledgement stream closed")?;
		validate_ack(&ack, sequence, &session_id)?;
		session_id = ack.session_id;
		debug!(target: "activation", %pod_uid, %session_id, sequence, phase = "publisher_ack", "demand snapshot accepted");
		tokio::select! {
			change = source.changed() => {
				if !change.urgent {
					tokio::time::sleep(COALESCE).await;
				}
			},
			_ = heartbeat.tick() => {},
		}
		sequence = sequence
			.checked_add(1)
			.context("demand sequence exhausted")?;
		let update = frame(source, pod_uid, sequence, &session_id)?;
		debug!(target: "activation", %pod_uid, %session_id, sequence, sampled_at = update.sampled_at_unix_millis, phase = "publisher_send", "publishing demand snapshot");
		tokio::time::timeout(ACK_TIMEOUT, tx.send(update))
			.await
			.context("demand send timed out")?
			.context("demand request stream closed")?;
	}
}

#[cfg(test)]
mod tests {
	use std::sync::{Arc, Mutex};

	use protos::activation::demand_publisher_server::{DemandPublisher, DemandPublisherServer};
	use tokio_stream::wrappers::TcpListenerStream;
	use tonic::{Request, Response, Status, Streaming};

	use super::*;
	use crate::proxy::backend_activity::{BackendActivity, Binding, Change, Snapshot};

	#[derive(Default)]
	struct TestSource {
		backends: Vec<BackendActivity>,
		changes: tokio::sync::Notify,
	}

	impl Source for TestSource {
		fn snapshot(&self) -> anyhow::Result<Snapshot> {
			Ok(Snapshot {
				process_generation: "test-generation".into(),
				backends: self.backends.clone(),
			})
		}

		fn changed(&self) -> BoxFuture<'_, Change> {
			Box::pin(async move {
				self.changes.notified().await;
				Change { urgent: false }
			})
		}
	}

	#[test]
	fn acknowledgements_are_sequence_and_session_fenced() {
		let ack = DemandAck {
			session_id: "current".into(),
			sequence: 4,
		};
		assert!(validate_ack(&ack, 4, "").is_ok());
		assert!(validate_ack(&ack, 4, "current").is_ok());
		assert!(validate_ack(&ack, 3, "current").is_err());
		assert!(validate_ack(&ack, 4, "old").is_err());
		assert!(validate_ack(&DemandAck::default(), 0, "").is_err());
	}

	#[test]
	fn frames_include_explicit_binding_and_protocol_identity() {
		let source = TestSource {
			backends: vec![BackendActivity {
				identity: "ns/service-uid/80".into(),
				outstanding: 7,
				binding: Binding {
					binding_uid: "binding-uid".into(),
					epoch: 4,
					draining: true,
				},
			}],
			..Default::default()
		};
		let sample = frame(&source, "pod-1", 7, "session-1").unwrap();
		assert_eq!(sample.protocol_version, 1);
		assert_eq!(sample.pod_uid, "pod-1");
		assert_eq!(sample.sequence, 7);
		assert_eq!(sample.session_id, "session-1");
		assert!(!sample.process_generation.is_empty());
		assert!(sample.sampled_at_unix_millis > 0);
		assert_eq!(sample.backends.len(), 1);
		assert_eq!(sample.backends[0].identity, "ns/service-uid/80");
		assert_eq!(sample.backends[0].outstanding, 7);
		assert_eq!(sample.backends[0].binding_uid, "binding-uid");
		assert_eq!(sample.backends[0].epoch, 4);
		assert!(sample.backends[0].draining);
	}

	#[test]
	fn frame_rejects_counts_outside_the_protocol_range() {
		let source = TestSource {
			backends: vec![BackendActivity {
				identity: "ns/service-uid/80".into(),
				outstanding: u64::MAX,
				binding: Binding::default(),
			}],
			..Default::default()
		};
		assert!(frame(&source, "pod-1", 1, "").is_err());
	}

	#[derive(Clone)]
	struct FakeAdapter {
		received: Arc<Mutex<Vec<DemandSnapshot>>>,
	}

	#[tonic::async_trait]
	impl DemandPublisher for FakeAdapter {
		type PublishStream = ReceiverStream<Result<DemandAck, Status>>;

		async fn publish(
			&self,
			request: Request<Streaming<DemandSnapshot>>,
		) -> Result<Response<Self::PublishStream>, Status> {
			let mut requests = request.into_inner();
			let received = self.received.clone();
			let (tx, rx) = mpsc::channel(1);
			tokio::spawn(async move {
				while let Ok(Some(frame)) = requests.message().await {
					let ack = DemandAck {
						session_id: "assigned-session".into(),
						sequence: frame.sequence,
					};
					received.lock().unwrap().push(frame);
					if tx.send(Ok(ack)).await.is_err() {
						break;
					}
				}
			});
			Ok(Response::new(ReceiverStream::new(rx)))
		}
	}

	#[tokio::test]
	async fn outbound_stream_registers_heartbeats_and_resynchronizes() {
		let received = Arc::new(Mutex::new(Vec::new()));
		let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
		let address = listener.local_addr().unwrap();
		let server = tokio::spawn(
			tonic::transport::Server::builder()
				.add_service(DemandPublisherServer::new(FakeAdapter {
					received: received.clone(),
				}))
				.serve_with_incoming(TcpListenerStream::new(listener)),
		);
		let endpoint = Endpoint::from_shared(format!("http://{address}")).unwrap();
		let source = Arc::new(TestSource::default());
		let publisher = tokio::spawn({
			let endpoint = endpoint.clone();
			let source = source.clone();
			async move { session(&endpoint, source.as_ref(), "test-pod").await }
		});
		tokio::time::timeout(Duration::from_secs(3), async {
			loop {
				if received.lock().unwrap().len() >= 2 {
					break;
				}
				tokio::time::sleep(Duration::from_millis(5)).await;
			}
		})
		.await
		.unwrap();
		publisher.abort();
		let _ = publisher.await;
		let first_len = received.lock().unwrap().len();
		let reconnect =
			tokio::spawn(async move { session(&endpoint, source.as_ref(), "test-pod").await });
		tokio::time::timeout(Duration::from_secs(3), async {
			loop {
				if received.lock().unwrap().len() > first_len {
					break;
				}
				tokio::time::sleep(Duration::from_millis(5)).await;
			}
		})
		.await
		.unwrap();
		{
			let frames = received.lock().unwrap();
			assert_eq!(frames[0].sequence, 1);
			assert!(frames[0].session_id.is_empty());
			assert_eq!(frames[1].session_id, "assigned-session");
			assert!(frames[1].sampled_at_unix_millis - frames[0].sampled_at_unix_millis >= 400);
			assert_eq!(frames[first_len].sequence, 1);
			assert!(frames[first_len].session_id.is_empty());
		}
		reconnect.abort();
		let _ = reconnect.await;
		server.abort();
		let _ = server.await;
	}
}
