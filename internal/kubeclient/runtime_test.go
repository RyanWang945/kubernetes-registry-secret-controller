package kubeclient

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/RyanWang945/kubernetes-registry-secret-controller/internal/config"
)

func TestRuntimeKeepsWorkersUntilRestartAndRestoresFallback(t *testing.T) {
	fallback := Settings{QPS: 25, Burst: 50, NamespaceWorkers: 8, ServiceAccountWorkers: 12}
	initial := config.RuntimeConfiguration{KubeAPIQPS: 40, KubeAPIBurst: 80, Workers: 4}
	r, err := NewRuntime(fallback, initial)
	if err != nil {
		t.Fatal(err)
	}
	bucket := r.limiter.bucket
	var output bytes.Buffer
	ctx := log.IntoContext(context.Background(), logr.FromSlogHandler(slog.NewJSONHandler(&output, nil)))
	next := config.RuntimeConfiguration{KubeAPIQPS: 60, KubeAPIBurst: 120, Workers: 16}
	for range 2 {
		if err := r.Apply(ctx, next); err != nil {
			t.Fatal(err)
		}
	}
	if r.Current() != (Settings{QPS: 60, Burst: 120, NamespaceWorkers: 4, ServiceAccountWorkers: 4}) {
		t.Fatal("workers changed without restart, or limit was not applied")
	}
	if r.limiter.bucket != bucket {
		t.Fatal("hot update replaced the token bucket")
	}
	if strings.Count(output.String(), `"operation":"configure_workers"`) != 1 || !strings.Contains(output.String(), `"restart_required":true`) {
		t.Fatal("worker restart requirement was not logged once")
	}
	if err := r.Apply(ctx, config.RuntimeConfiguration{KubeAPIQPS: 99, KubeAPIBurst: -1}); err == nil || r.Current().QPS != 60 {
		t.Fatal("invalid pair partially changed runtime values")
	}
	if err := r.Apply(ctx, config.RuntimeConfiguration{}); err != nil {
		t.Fatal(err)
	}
	if r.Current() != (Settings{QPS: 25, Burst: 50, NamespaceWorkers: 4, ServiceAccountWorkers: 4}) {
		t.Fatal("field deletion did not restore startup defaults")
	}
	restarted, err := NewRuntime(fallback, next)
	if err != nil {
		t.Fatal(err)
	}
	if restarted.Current().NamespaceWorkers != 16 || restarted.Current().ServiceAccountWorkers != 16 {
		t.Fatal("restart did not adopt configured workers")
	}
}

func TestLimiterPreservesDebtAndChangesFutureAdmission(t *testing.T) {
	l := newLimiter(0.000001, 2)
	if !l.TryAccept() || !l.TryAccept() || l.TryAccept() {
		t.Fatal("incorrect initial burst")
	}
	l.set(0.000001, 20)
	if l.TryAccept() {
		t.Fatal("raising burst refilled consumed tokens")
	}
	l.set(100, 20)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := l.Wait(ctx); err != nil {
		t.Fatalf("raised QPS did not affect new requests: %v", err)
	}
	l.set(0.000001, 1)
	// Any accumulated credit is capped by the new burst on the next admission.
	l.TryAccept()
	short, stop := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer stop()
	if err := l.Wait(short); err == nil {
		t.Fatal("lowered limit did not throttle new requests")
	}
	canceled, stopCanceled := context.WithCancel(context.Background())
	stopCanceled()
	if !errors.Is(l.Wait(canceled), context.Canceled) {
		t.Fatal("request cancellation was not preserved")
	}
}

func TestLimiterConcurrentWaitAndUpdate(t *testing.T) {
	l := newLimiter(10000, 100)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 40 {
				l.set(10000, 50)
				if err := l.Wait(ctx); err != nil {
					t.Error(err)
					return
				}
				l.set(20000, 100)
				_ = l.QPS()
			}
		})
	}
	wg.Wait()
}

func TestBootstrapUsesOnlyFullyValidConfiguration(t *testing.T) {
	fallback := Settings{QPS: 25, Burst: 50, NamespaceWorkers: 8, ServiceAccountWorkers: 8}
	cm := &corev1.ConfigMap{Data: map[string]string{
		"namespace": "*", "serviceaccount": "default",
		"registries": `[{"regionID":"cn-test","instanceID":"cri-a","accessKeyID":"test-key","accessKeySecret":"test-secret","domains":["registry.example.com"]}]`,
		"kubeAPIQPS": "40", "kubeAPIBurst": "80", "workers": "16",
	}}
	key := client.ObjectKey{Namespace: "system", Name: "configuration"}
	r, err := runtimeFromConfigMap(context.Background(), cm, key, fallback)
	if err != nil || r.Current() != (Settings{QPS: 40, Burst: 80, NamespaceWorkers: 16, ServiceAccountWorkers: 16}) {
		t.Fatal("valid startup values were ignored")
	}
	cm.Data["registries"] = "sensitive-invalid-config"
	var output bytes.Buffer
	ctx := log.IntoContext(context.Background(), logr.FromSlogHandler(slog.NewJSONHandler(&output, nil)))
	r, err = runtimeFromConfigMap(ctx, cm, key, fallback)
	if err != nil || r.Current() != fallback {
		t.Fatal("invalid business config partially applied startup tuning")
	}
	if strings.Contains(output.String(), "sensitive") {
		t.Fatal("bootstrap leaked configuration data")
	}
	r, err = runtimeFromConfigMap(ctx, nil, key, fallback)
	if err != nil || r.Current() != fallback {
		t.Fatal("missing config did not use startup defaults")
	}
}
