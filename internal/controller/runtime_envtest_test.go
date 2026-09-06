//go:build integration

package controller

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-logr/logr"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlconfig "sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/RyanWang945/kubernetes-registry-secret-controller/internal/config"
	"github.com/RyanWang945/kubernetes-registry-secret-controller/internal/kubeclient"
)

func TestRuntimeHotReloadWithRealClientsAndWorkerRestart(t *testing.T) {
	environment := &envtest.Environment{DownloadBinaryAssets: true, DownloadBinaryAssetsVersion: envtestKubernetesVersion, BinaryAssetsDirectory: filepath.Join(repositoryRoot(t), ".cache", "envtest")}
	restConfig, err := environment.Start()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := environment.Stop(); err != nil {
			t.Error(err)
		}
	})
	restConfig.QPS, restConfig.Burst = 25, 50
	api, err := client.New(restConfig, client.Options{})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for _, name := range []string{DefaultControllerNamespace, "production"} {
		if err := api.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}); err != nil {
			t.Fatal(err)
		}
	}
	cm := testConfigMap("production", "default")
	cm.Data[config.KubeAPIQPSKey], cm.Data[config.KubeAPIBurstKey], cm.Data[config.WorkersKey] = "100", "1", "2"
	if err := api.Create(ctx, cm); err != nil {
		t.Fatal(err)
	}
	key := client.ObjectKeyFromObject(cm)
	type replica struct {
		manager  ctrl.Manager
		runtime  *kubeclient.Runtime
		store    *config.Store
		observer *recordingConfigurationObserver
		cancel   context.CancelFunc
	}
	startReplica := func() replica {
		t.Helper()
		runtime, err := kubeclient.Bootstrap(ctx, restConfig, key, kubeclient.Settings{QPS: 25, Burst: 50, NamespaceWorkers: 8, ServiceAccountWorkers: 8})
		if err != nil {
			t.Fatal(err)
		}
		current := runtime.Current()
		options := ControllerOptions{RuntimeUpdater: runtime, MaxConcurrentNamespaceReconciles: current.NamespaceWorkers, MaxConcurrentServiceAccountReconciles: current.ServiceAccountWorkers}
		cacheOptions, err := NewCacheOptions(options)
		if err != nil {
			t.Fatal(err)
		}
		lease, renew, retry := 3*time.Second, 2*time.Second, 500*time.Millisecond
		manager, err := ctrl.NewManager(restConfig, ctrl.Options{
			Cache: cacheOptions, NewClient: runtime.NewClient, Logger: logr.Discard(),
			Controller: ctrlconfig.Controller{SkipNameValidation: ptr.To(true)},
			Metrics:    metricsserver.Options{BindAddress: "0"}, HealthProbeBindAddress: "0",
			LeaderElection: true, LeaderElectionID: "runtime-tuning", LeaderElectionNamespace: DefaultControllerNamespace,
			LeaderElectionReleaseOnCancel: true, LeaseDuration: &lease, RenewDeadline: &renew, RetryPeriod: &retry,
		})
		if err != nil {
			t.Fatal(err)
		}
		store := &config.Store{}
		observer := &recordingConfigurationObserver{}
		publisher := mustResourceEventPublisher(t, manager.GetClient())
		if err := SetupWithManager(manager, store, observer, publisher, newRecordingSyncer(nil), newRecordingSyncer(nil), options); err != nil {
			t.Fatal(err)
		}
		runCtx, cancel := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() { done <- manager.Start(runCtx) }()
		t.Cleanup(func() {
			cancel()
			select {
			case err := <-done:
				if err != nil {
					t.Error(err)
				}
			case <-time.After(10 * time.Second):
				t.Error("Manager did not stop")
			}
		})
		return replica{manager, runtime, store, observer, cancel}
	}
	replicas := []replica{startReplica(), startReplica()}
	eventually(t, 10*time.Second, func() bool { return replicas[0].store.Loaded() && replicas[1].store.Loaded() }, "replicas did not load configuration")
	leader := -1
	eventually(t, 10*time.Second, func() bool {
		for i, r := range replicas {
			select {
			case <-r.manager.Elected():
				leader = i
				return true
			default:
			}
		}
		return false
	}, "leader not elected")
	update := func(qps, burst, workers string) {
		t.Helper()
		cm := &corev1.ConfigMap{}
		if err := api.Get(ctx, key, cm); err != nil {
			t.Fatal(err)
		}
		cm.Data[config.KubeAPIQPSKey], cm.Data[config.KubeAPIBurstKey], cm.Data[config.WorkersKey] = qps, burst, workers
		if err := api.Update(ctx, cm); err != nil {
			t.Fatal(err)
		}
	}
	update("0.001", "1", "4")
	eventually(t, 10*time.Second, func() bool {
		return replicas[0].runtime.Current().QPS == 0.001 && replicas[1].runtime.Current().QPS == 0.001
	}, "Leader and Follower did not reload client limits")
	for _, r := range replicas {
		if r.runtime.Current().NamespaceWorkers != 2 || r.runtime.Current().ServiceAccountWorkers != 2 || r.observer.count() != 1 {
			t.Fatal("hot tuning changed workers or notified the credential scheduler")
		}
	}
	if restConfig.RateLimiter != nil || restConfig.QPS != 25 || restConfig.Burst != 50 {
		t.Fatal("business limiter contaminated shared Manager REST config")
	}
	write := func(ctx context.Context, name string) error {
		return replicas[leader].manager.GetClient().Create(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "production", Name: name}})
	}
	firstWriteCtx, cancelFirstWrite := context.WithTimeout(ctx, 2*time.Second)
	if err := write(firstWriteCtx, "consume-burst"); err != nil {
		t.Fatal(err)
	}
	cancelFirstWrite()
	short, cancelShort := context.WithTimeout(ctx, 50*time.Millisecond)
	if err := write(short, "must-be-throttled"); err == nil {
		t.Error("real client bypassed updated rate limit")
	}
	cancelShort()
	leaseKey := client.ObjectKey{Namespace: DefaultControllerNamespace, Name: "runtime-tuning"}
	lease := &coordinationv1.Lease{}
	if err := api.Get(ctx, leaseKey, lease); err != nil {
		t.Fatal(err)
	}
	if lease.Spec.RenewTime == nil {
		t.Fatal("leader Lease has no renewal time")
	}
	before := lease.Spec.RenewTime.DeepCopy()
	eventually(t, 5*time.Second, func() bool {
		if err := api.Get(ctx, leaseKey, lease); err != nil {
			return false
		}
		return lease.Spec.RenewTime != nil && lease.Spec.RenewTime.After(before.Time)
	}, "business throttling blocked Lease renewal")
	update("100", "50", "4")
	eventually(t, 5*time.Second, func() bool {
		return replicas[0].runtime.Current().QPS == 100 && replicas[1].runtime.Current().QPS == 100
	}, "business throttling blocked configuration watch")
	writeCtx, cancelWrite := context.WithTimeout(ctx, 2*time.Second)
	if err := write(writeCtx, "after-hot-increase"); err != nil {
		t.Errorf("existing client did not adopt increased QPS: %v", err)
	}
	cancelWrite()
	update("500", "100", "0")
	eventually(t, 5*time.Second, func() bool { return !replicas[0].store.Valid() && !replicas[1].store.Valid() }, "invalid configuration was not observed")
	for _, r := range replicas {
		if r.runtime.Current().QPS != 100 {
			t.Fatal("invalid configuration partially applied")
		}
	}
	update("100", "50", "4")
	eventually(t, 5*time.Second, func() bool { return replicas[0].store.Valid() && replicas[1].store.Valid() }, "valid configuration not restored")
	for _, r := range replicas {
		r.cancel()
	}
	restarted := startReplica()
	eventually(t, 10*time.Second, func() bool {
		select {
		case <-restarted.manager.Elected():
			return true
		default:
			return false
		}
	}, "restarted replica did not become leader")
	eventually(t, 5*time.Second, func() bool {
		families, err := ctrlmetrics.Registry.Gather()
		if err != nil {
			return false
		}
		found := 0
		for _, family := range families {
			if family.GetName() != "controller_runtime_max_concurrent_reconciles" {
				continue
			}
			for _, metric := range family.Metric {
				for _, label := range metric.Label {
					if label.GetName() == "controller" && (label.GetValue() == "namespace_secrets" || label.GetValue() == "service_accounts") && metric.GetGauge().GetValue() == 4 {
						found++
					}
				}
			}
		}
		return found == 2
	}, "restart did not create four workers in each resource controller")
}
