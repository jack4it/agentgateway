use std::collections::HashMap;
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::Duration;

use futures::future::BoxFuture;
use prometheus_client::collector::Collector;
use prometheus_client::encoding::{DescriptorEncoder, EncodeLabelSet, EncodeMetric};
use prometheus_client::metrics::gauge::ConstGauge;
use serde::Serialize;
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};

use super::admission::{Limiter, Permit};
pub use super::backend_activity::Binding;
use super::backend_activity::{BackendActivity, Change, Snapshot, Source};

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Policy {
	pub identity: String,
	#[serde(with = "crate::serdes::serde_dur")]
	pub max_wait: Duration,
	pub max_pending_requests: NonZeroU32,
	#[serde(flatten)]
	pub binding: Binding,
	#[serde(skip)]
	pub state: Arc<State>,
}

#[derive(Debug, Default)]
pub struct State {
	outstanding: AtomicU64,
	binding: Mutex<Binding>,
	admission: Limiter,
	dispatch: OnceLock<Arc<Semaphore>>,
}

type Registry = Mutex<HashMap<String, Weak<State>>>;
static STATES: OnceLock<Registry> = OnceLock::new();
static GLOBAL_ADMISSION: OnceLock<Limiter> = OnceLock::new();
static CHANGES: OnceLock<Notify> = OnceLock::new();
static URGENT: AtomicBool = AtomicBool::new(false);

pub fn notify_change() {
	CHANGES.get_or_init(Notify::new).notify_one();
}

pub(crate) struct GatewayActivitySource {
	stores: crate::store::Stores,
}

impl GatewayActivitySource {
	pub(crate) fn new(stores: crate::store::Stores) -> Self {
		Self { stores }
	}
}

impl Source for GatewayActivitySource {
	fn snapshot(&self) -> anyhow::Result<Snapshot> {
		Ok(Snapshot {
			backends: backend_counts(&self.stores),
			process_generation: process_generation().to_owned(),
		})
	}

	fn changed(&self) -> BoxFuture<'_, Change> {
		Box::pin(async {
			CHANGES.get_or_init(Notify::new).notified().await;
			Change {
				urgent: URGENT.swap(false, Ordering::AcqRel),
			}
		})
	}
}

impl Policy {
	pub fn matches_service(&self, service: &crate::types::discovery::Service, port: u16) -> bool {
		service.kubernetes_uid.as_ref().is_some_and(|uid| {
			!uid.is_empty() && self.identity == format!("{}/{uid}/{port}", service.namespace)
		})
	}
	pub fn new(
		identity: String,
		max_wait: Duration,
		max_pending_requests: u32,
	) -> anyhow::Result<Self> {
		anyhow::ensure!(!identity.is_empty(), "activation identity is required");
		anyhow::ensure!(!max_wait.is_zero(), "activation maxWait must be positive");
		let max_pending_requests = NonZeroU32::new(max_pending_requests)
			.ok_or_else(|| anyhow::anyhow!("activation maxPendingRequests must be positive"))?;
		let mut states = STATES
			.get_or_init(Default::default)
			.lock()
			.expect("activation registry");
		states.retain(|_, state| state.strong_count() > 0);
		let state = states
			.get(&identity)
			.and_then(Weak::upgrade)
			.unwrap_or_else(|| {
				let state = Arc::new(State::default());
				states.insert(identity.clone(), Arc::downgrade(&state));
				state
			});
		Ok(Self {
			identity,
			max_wait,
			max_pending_requests,
			binding: Binding::default(),
			state,
		})
	}

	pub fn with_binding(
		mut self,
		binding_uid: String,
		epoch: u64,
		draining: bool,
	) -> anyhow::Result<Self> {
		anyhow::ensure!(
			(binding_uid.is_empty() && epoch == 0 && !draining) || (!binding_uid.is_empty() && epoch > 0),
			"activation binding UID and positive epoch must be configured together"
		);
		self.binding = Binding {
			binding_uid,
			epoch,
			draining,
		};
		Ok(self)
	}

	pub fn activate(&self) {
		*self.state.binding.lock().expect("activation binding") = self.binding.clone();
		notify_change();
	}

	pub fn is_draining(&self) -> bool {
		self.binding.draining
			|| self
				.state
				.binding
				.lock()
				.expect("activation binding")
				.draining
	}

	pub fn admit(&self) -> Option<Arc<Demand>> {
		let binding = self.state.binding.lock().expect("activation binding");
		if *binding != self.binding || binding.draining {
			return None;
		}
		let global = GLOBAL_ADMISSION
			.get_or_init(Default::default)
			.try_acquire(NonZeroU32::new(128))?;
		let backend = self
			.state
			.admission
			.try_acquire(Some(self.max_pending_requests))?;
		let previous = self.state.outstanding.fetch_add(1, Ordering::AcqRel);
		drop(binding);
		if previous == 0 {
			URGENT.store(true, Ordering::Release);
		}
		notify_change();
		Some(Arc::new(Demand {
			state: self.state.clone(),
			_backend: backend,
			_global: global,
		}))
	}

	pub async fn dispatch(&self) -> OwnedSemaphorePermit {
		self
			.state
			.dispatch
			.get_or_init(|| Arc::new(Semaphore::new(10)))
			.clone()
			.acquire_owned()
			.await
			.expect("activation dispatch semaphore remains open")
	}

	pub fn count(&self) -> u64 {
		self.state.outstanding.load(Ordering::Acquire)
	}
}

pub struct Demand {
	state: Arc<State>,
	_backend: Permit,
	_global: Permit,
}

impl Drop for Demand {
	fn drop(&mut self) {
		self.state.outstanding.fetch_sub(1, Ordering::AcqRel);
		notify_change();
	}
}

#[derive(Clone)]
pub struct ConfigurationRevision(pub u64);

#[derive(Clone)]
pub struct RequestDemand {
	pub identity: String,
	pub demand: Arc<Demand>,
	pub dispatch: Arc<OwnedSemaphorePermit>,
}

#[derive(Debug)]
struct DemandCollector(crate::store::Stores);

#[derive(EncodeLabelSet)]
struct DemandLabels {
	identity: String,
	binding_uid: String,
	epoch: u64,
	draining: bool,
}

pub fn register_metrics(
	registry: &mut prometheus_client::registry::Registry,
	stores: crate::store::Stores,
) {
	registry.register_collector(Box::new(DemandCollector(stores)));
}

impl Collector for DemandCollector {
	fn encode(&self, mut encoder: DescriptorEncoder) -> Result<(), std::fmt::Error> {
		let mut metric = encoder.encode_descriptor(
			"agentgateway_activation_outstanding_requests",
			"Local pending and executing activation requests",
			None,
			ConstGauge::new(0u64).metric_type(),
		)?;
		for backend in backend_counts(&self.0) {
			let labels = DemandLabels {
				identity: backend.identity,
				binding_uid: backend.binding.binding_uid,
				epoch: backend.binding.epoch,
				draining: backend.binding.draining,
			};
			ConstGauge::new(backend.outstanding).encode(metric.encode_family(&labels)?)?;
		}
		Ok(())
	}
}

fn process_generation() -> &'static str {
	static GENERATION: OnceLock<String> = OnceLock::new();
	GENERATION.get_or_init(|| uuid::Uuid::new_v4().to_string())
}

fn backend_counts(stores: &crate::store::Stores) -> Vec<BackendActivity> {
	let binds = stores.read_binds();
	let policies = binds.all_policies();
	let mut backends = HashMap::new();
	for targeted in policies {
		if let Some(crate::types::agent::BackendTrafficPolicy::Activation(policy)) =
			targeted.policy.as_backend()
		{
			backends.insert(
				policy.identity.clone(),
				BackendActivity {
					identity: policy.identity.clone(),
					outstanding: policy.count(),
					binding: policy.binding.clone(),
				},
			);
		}
	}
	let mut registry = STATES
		.get_or_init(Default::default)
		.lock()
		.expect("activation registry");
	registry.retain(|_, state| state.strong_count() > 0);
	for (identity, state) in registry.iter() {
		if !backends.contains_key(identity)
			&& let Some(state) = state.upgrade()
		{
			let mut binding = state.binding.lock().expect("activation binding").clone();
			let outstanding = state.outstanding.load(Ordering::Acquire);
			if outstanding > 0 {
				binding.draining = true;
				backends.insert(
					identity.clone(),
					BackendActivity {
						identity: identity.clone(),
						outstanding,
						binding,
					},
				);
			}
		}
	}
	backends.into_values().collect()
}

pub fn configured(stores: &crate::store::Stores, identity: &str) -> bool {
	stores.read_binds().all_policies().iter().any(|targeted| {
		matches!(targeted.policy.as_backend(),
			Some(crate::types::agent::BackendTrafficPolicy::Activation(policy))
			if policy.identity == identity && !policy.is_draining())
	})
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn rejects_invalid_configuration() {
		assert!(Policy::new(String::new(), Duration::from_secs(1), 1).is_err());
		assert!(Policy::new("empty-wait".into(), Duration::ZERO, 1).is_err());
		assert!(Policy::new("empty-limit".into(), Duration::from_secs(1), 0).is_err());
	}

	#[test]
	fn service_identity_rejects_replacement_missing_uid_and_wrong_port() {
		let policy = Policy::new("ns/original/80".into(), Duration::from_secs(1), 1).unwrap();
		let mut service = crate::types::discovery::Service {
			namespace: "ns".into(),
			kubernetes_uid: Some("original".into()),
			..Default::default()
		};
		assert!(policy.matches_service(&service, 80));
		assert!(!policy.matches_service(&service, 81));
		service.kubernetes_uid = Some("replacement".into());
		assert!(!policy.matches_service(&service, 80));
		service.kubernetes_uid = None;
		assert!(!policy.matches_service(&service, 80));
	}

	#[test]
	fn demand_is_bounded_shared_and_released_on_last_clone() {
		let policy = Policy::new("accounting-test".into(), Duration::from_secs(1), 1).unwrap();
		let updated = Policy::new("accounting-test".into(), Duration::from_secs(2), 1).unwrap();
		let demand = policy.admit().unwrap();
		assert_eq!(updated.count(), 1);
		assert!(policy.admit().is_none());
		let body_guard = demand.clone();
		drop(demand);
		assert_eq!(policy.count(), 1);
		drop(body_guard);
		assert_eq!(policy.count(), 0);
		assert!(policy.admit().is_some());
	}

	#[test]
	fn binding_configuration_requires_identity_and_epoch_together() {
		let make = || Policy::new("binding-validation".into(), Duration::from_secs(1), 1).unwrap();
		assert!(make().with_binding("binding".into(), 0, false).is_err());
		assert!(make().with_binding(String::new(), 1, false).is_err());
		assert!(make().with_binding(String::new(), 0, true).is_err());
		assert!(make().with_binding("binding".into(), 2, false).is_ok());
	}

	#[test]
	fn metrics_and_activity_source_include_configured_zero_and_current_demand() {
		use crate::types::agent::{
			BackendTarget, BackendTrafficPolicy, PolicyTarget, PolicyType, TargetedPolicy,
		};
		let stores = crate::store::Stores::default();
		let policy = Arc::new(
			Policy::new("metrics-backend".into(), Duration::from_secs(1), 64)
				.unwrap()
				.with_binding("metrics-binding".into(), 2, false)
				.unwrap(),
		);
		stores.binds.write().insert_policy(TargetedPolicy {
			key: "metrics-policy".into(),
			name: None,
			target: PolicyTarget::Backend(BackendTarget::Backend {
				name: "backend".into(),
				namespace: "test".into(),
				section: None,
				kind: None,
			}),
			creation_timestamp: 0,
			inheritance: Default::default(),
			policy: PolicyType::Backend(BackendTrafficPolicy::Activation(policy.clone())),
		});
		let source = GatewayActivitySource::new(stores.clone());
		let initial = source.snapshot().unwrap();
		let backend = initial
			.backends
			.iter()
			.find(|backend| backend.identity == policy.identity)
			.unwrap();
		assert_eq!(backend.outstanding, 0);
		assert_eq!(backend.binding, policy.binding);
		assert!(!initial.process_generation.is_empty());
		let mut registry = prometheus_client::registry::Registry::default();
		register_metrics(&mut registry, stores);
		let encode = || {
			let mut text = String::new();
			prometheus_client::encoding::text::encode(&mut text, &registry).unwrap();
			text
		};
		let sample = "agentgateway_activation_outstanding_requests{identity=\"metrics-backend\",binding_uid=\"metrics-binding\",epoch=\"2\",draining=\"false\"}";
		assert!(encode().contains(&format!("{sample} 0\n")));
		let demand = policy.admit().unwrap();
		assert!(encode().contains(&format!("{sample} 1\n")));
		let current = source.snapshot().unwrap();
		assert_eq!(current.process_generation, initial.process_generation);
		assert_eq!(
			current
				.backends
				.iter()
				.find(|b| b.identity == policy.identity)
				.unwrap()
				.outstanding,
			1
		);
		drop(demand);
		assert!(encode().contains(&format!("{sample} 0\n")));
	}

	#[test]
	fn drain_commit_fences_old_admission_and_retains_executing_identity() {
		let active = Policy::new("drain-retention".into(), Duration::from_secs(1), 64)
			.unwrap()
			.with_binding("binding-drain".into(), 2, false)
			.unwrap();
		active.activate();
		let response = active.admit().unwrap();
		let draining = Policy::new("drain-retention".into(), Duration::from_secs(1), 64)
			.unwrap()
			.with_binding("binding-drain".into(), 3, true)
			.unwrap();
		assert!(
			active.admit().is_some(),
			"parsing alone must not change committed configuration"
		);
		draining.activate();
		assert!(active.admit().is_none());
		assert!(draining.admit().is_none());
		let stores = crate::store::Stores::default();
		let source = GatewayActivitySource::new(stores.clone());
		let sample = source.snapshot().unwrap();
		let retained = sample
			.backends
			.iter()
			.find(|b| b.identity == "drain-retention")
			.unwrap();
		assert_eq!(retained.outstanding, 1);
		assert_eq!(retained.binding.epoch, 3);
		assert!(retained.binding.draining);
		let mut metrics = prometheus_client::registry::Registry::default();
		register_metrics(&mut metrics, stores.clone());
		let mut text = String::new();
		prometheus_client::encoding::text::encode(&mut text, &metrics).unwrap();
		assert!(text.contains(
			"identity=\"drain-retention\",binding_uid=\"binding-drain\",epoch=\"3\",draining=\"true\"} 1\n"
		));
		drop(response);
		assert_eq!(draining.count(), 0);
		assert!(
			source
				.snapshot()
				.unwrap()
				.backends
				.iter()
				.all(|b| b.identity != "drain-retention")
		);
	}

	#[tokio::test]
	async fn dispatch_is_bounded_and_cancel_safe() {
		let policy = Policy::new("dispatch-test".into(), Duration::from_secs(1), 64).unwrap();
		let mut permits = Vec::new();
		for _ in 0..10 {
			permits.push(policy.dispatch().await);
		}
		assert!(
			tokio::time::timeout(Duration::from_millis(10), policy.dispatch())
				.await
				.is_err()
		);
		drop(permits.pop());
		assert!(
			tokio::time::timeout(Duration::from_millis(10), policy.dispatch())
				.await
				.is_ok()
		);
	}
}
