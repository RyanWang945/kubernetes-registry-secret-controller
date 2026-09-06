package controller

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/RyanWang945/kubernetes-registry-secret-controller/internal/config"
	"github.com/RyanWang945/kubernetes-registry-secret-controller/internal/kubeclient"
)

func TestRuntimeChangesDoNotFanOutAndPreservePendingBusinessRetry(t *testing.T) {
	ctx := testContext()
	cm := testConfigMap("production", "default")
	key := client.ObjectKeyFromObject(cm)
	kube := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(cm,
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "production"}},
		&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Namespace: "production", Name: "default"}},
	).Build()
	runtime, err := kubeclient.NewRuntime(kubeclient.Settings{QPS: 25, Burst: 50, NamespaceWorkers: 8, ServiceAccountWorkers: 8}, config.RuntimeConfiguration{})
	if err != nil {
		t.Fatal(err)
	}
	store := &config.Store{}
	publisher := mustResourceEventPublisher(t, kube)
	observer := &recordingConfigurationObserver{}
	options := ControllerOptions{RuntimeUpdater: runtime}.withDefaults()
	r := &ConfigurationReconciler{client: kube, store: store, observer: observer, publisher: publisher, options: options}
	reconcile := func() error { _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key}); return err }
	update := func(data map[string]string) {
		t.Helper()
		current := &corev1.ConfigMap{}
		if err := kube.Get(ctx, key, current); err != nil {
			t.Fatal(err)
		}
		current.Data = data
		if err := kube.Update(ctx, current); err != nil {
			t.Fatal(err)
		}
	}
	if err := reconcile(); err != nil {
		t.Fatal(err)
	}
	assertNamespaceEvents(t, publisher.namespaceEvents, "production")
	assertServiceAccountEvents(t, publisher.serviceAccountEvents, "production/default")
	data := testConfigData("production", "default")
	data[config.KubeAPIQPSKey], data[config.KubeAPIBurstKey], data[config.WorkersKey] = "40", "80", "16"
	update(data)
	if err := reconcile(); err != nil {
		t.Fatal(err)
	}
	if runtime.Current() != (kubeclient.Settings{QPS: 40, Burst: 80, NamespaceWorkers: 8, ServiceAccountWorkers: 8}) {
		t.Fatal("runtime tuning was not applied correctly")
	}
	assertNoEvent(t, publisher.namespaceEvents, "Namespace")
	assertNoEvent(t, publisher.serviceAccountEvents, "ServiceAccount")
	if observer.count() != 1 {
		t.Fatal("tuning update notified credential scheduler")
	}
	data[config.KubeAPIQPSKey], data[config.WorkersKey] = "60", "0"
	update(data)
	if err := reconcile(); err != nil {
		t.Fatal(err)
	}
	if store.Valid() || runtime.Current().QPS != 40 {
		t.Fatal("invalid worker update partially applied QPS")
	}
	if err := kube.Get(ctx, key, cm); err != nil {
		t.Fatal(err)
	}
	if err := kube.Delete(ctx, cm); err != nil {
		t.Fatal(err)
	}
	if err := reconcile(); err != nil {
		t.Fatal(err)
	}
	if runtime.Current().QPS != 40 || !store.Loaded() {
		t.Fatal("ConfigMap deletion lost last good tuning")
	}
	cm = testConfigMap("production", "default")
	if err := kube.Create(ctx, cm); err != nil {
		t.Fatal(err)
	}
	if err := reconcile(); err != nil {
		t.Fatal(err)
	}
	if runtime.Current().QPS != 25 || runtime.Current().Burst != 50 || !store.Valid() {
		t.Fatal("field removal did not restore fallback")
	}
	assertNoEvent(t, publisher.namespaceEvents, "Namespace")
	assertNoEvent(t, publisher.serviceAccountEvents, "ServiceAccount")

	// Fail after Store.Apply, then receive a tuning-only edit before the retry.
	// The old business publication still has to finish.
	publisher.reader = &failOnceNamespaceListClient{Client: kube}
	data = testConfigData("production,staging", "default")
	update(data)
	if err := reconcile(); err == nil {
		t.Fatal("expected publication failure")
	}
	data[config.KubeAPIQPSKey] = "70"
	update(data)
	if err := reconcile(); err != nil {
		t.Fatal(err)
	}
	assertNamespaceEvents(t, publisher.namespaceEvents, "production")
	assertServiceAccountEvents(t, publisher.serviceAccountEvents, "production/default")
	if observer.count() != 2 || runtime.Current().QPS != 70 {
		t.Fatal("business retry was lost or repeated credential notification")
	}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key}); err != nil {
		t.Fatal(err)
	}
	assertNoEvent(t, publisher.namespaceEvents, "Namespace")
}
