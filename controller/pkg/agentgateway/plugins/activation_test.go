package plugins

import (
	"fmt"
	"testing"
	"time"

	"istio.io/istio/pkg/kube/krt"
	"istio.io/istio/pkg/ptr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	gwv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/agentgateway/agentgateway/controller/api/v1alpha1/agentgateway"
)

func TestTranslateActivation(t *testing.T) {
	service := &corev1.Service{
		Name: "app", Namespace: "apps", UID: types.UID("service-uid"),
		Spec: corev1.ServiceSpec{Ports: []corev1.ServicePort{{Port: 80}}},
	}

	ctx := PolicyCtx{
		Krt: krt.TestingDummyContext{},
		Collections: &AgwCollections{
			Services: krt.NewStaticCollection[*corev1.Service](nil, []*corev1.Service{service}),
		},
	}
	policy := &agentgateway.AgentgatewayPolicy{
		Name: "activation", Namespace: "apps",
		Spec: agentgateway.AgentgatewayPolicySpec{
			TargetRefs: []agentgateway.LocalPolicyTargetReferenceWithSectionName{{
				Kind: "Service", Name: "app", SectionName: ptr.Of(gwv1.SectionName("80")),
			}},
			Backend: &agentgateway.BackendFull{
				Activation: &agentgateway.BackendActivation{
					MaxWait:            agentgateway.Duration{Duration: 30 * time.Second},
					MaxPendingRequests: 64,
				},
			},
		},
	}
	got, err := translateBackendActivation(ctx, policy)
	if err != nil {
		t.Fatal(err)
	}
	activation := got.GetBackend().GetActivation()
	if activation.Identity != "apps/service-uid/80" || activation.MaxWait.AsDuration() != 30*time.Second ||
		activation.MaxPendingRequests != 64 {
		t.Fatalf("unexpected activation: %v", activation)
	}
	for name, change := range map[string]func(*agentgateway.AgentgatewayPolicy){
		"no target":    func(p *agentgateway.AgentgatewayPolicy) { p.Spec.TargetRefs = nil },
		"non-Service":  func(p *agentgateway.AgentgatewayPolicy) { p.Spec.TargetRefs[0].Kind = "HTTPRoute" },
		"missing port": func(p *agentgateway.AgentgatewayPolicy) { p.Spec.TargetRefs[0].SectionName = nil },
		"unknown port": func(p *agentgateway.AgentgatewayPolicy) {
			p.Spec.TargetRefs[0].SectionName = ptr.Of(gwv1.SectionName("81"))
		},
		"missing Service": func(p *agentgateway.AgentgatewayPolicy) { p.Spec.TargetRefs[0].Name = "absent" },
		"zero wait":       func(p *agentgateway.AgentgatewayPolicy) { p.Spec.Backend.Activation.MaxWait.Duration = 0 },
		"zero limit":      func(p *agentgateway.AgentgatewayPolicy) { p.Spec.Backend.Activation.MaxPendingRequests = 0 },
		"large limit":     func(p *agentgateway.AgentgatewayPolicy) { p.Spec.Backend.Activation.MaxPendingRequests = 129 },
	} {
		t.Run(name, func(t *testing.T) {
			invalid := policy.DeepCopy()
			change(invalid)
			if _, err := translateBackendActivation(ctx, invalid); err == nil {
				t.Fatal("expected validation failure")
			}
		})
	}
}

func TestTranslateManagedActivation(t *testing.T) {
	for _, draining := range []bool{false, true} {
		for _, serviceUID := range []string{"original-uid", "replacement-uid", ""} {
			t.Run(fmt.Sprintf("draining=%v/service=%s", draining, serviceUID), func(t *testing.T) {
				var services []*corev1.Service
				if serviceUID != "" {
					services = append(services, &corev1.Service{Name: "app", Namespace: "apps", UID: types.UID(serviceUID), Spec: corev1.ServiceSpec{Ports: []corev1.ServicePort{{Port: 80}}}})
				}
				ctx := PolicyCtx{Krt: krt.TestingDummyContext{}, Collections: &AgwCollections{
					Services: krt.NewStaticCollection[*corev1.Service](nil, services),
				}}
				policy := &agentgateway.AgentgatewayPolicy{Name: "managed", Namespace: "apps",
					Spec: agentgateway.AgentgatewayPolicySpec{
						TargetRefs: []agentgateway.LocalPolicyTargetReferenceWithSectionName{{Kind: "Service", Name: "app", SectionName: ptr.Of(gwv1.SectionName("80"))}},
						Backend: &agentgateway.BackendFull{Activation: &agentgateway.BackendActivation{
							ServiceUID: "original-uid", BindingUID: "binding-uid", Epoch: 7, Draining: draining,
							MaxWait: agentgateway.Duration{Duration: 30 * time.Second}, MaxPendingRequests: 64,
						}},
					},
				}
				got, err := translateBackendActivation(ctx, policy)
				valid := draining || serviceUID == "original-uid"
				if !valid {
					if err == nil {
						t.Fatal("active binding silently rebound")
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				a := got.GetBackend().GetActivation()
				if a.Identity != "apps/original-uid/80" || a.BindingUid != "binding-uid" || a.Epoch != 7 || a.Draining != draining {
					t.Fatalf("lost binding fence: %+v", a)
				}
				policy.Spec.Backend.Activation.ServiceUID = ""
				if _, err := translateBackendActivation(ctx, policy); err == nil {
					t.Fatal("managed identity pin required")
				}
			})
		}
	}
}
