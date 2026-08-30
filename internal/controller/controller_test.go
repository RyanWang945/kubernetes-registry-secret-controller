package controller

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/RyanWang945/kubernetes-registry-secret-controller/internal/config"
	"github.com/RyanWang945/kubernetes-registry-secret-controller/internal/registrysecret"
)

const testRegistries = `
- regionID: cn-hangzhou
  instanceID: cri-test
  accessKeyID: key-test
  accessKeySecret: secret-test
  domains:
    - registry-test.cn-hangzhou.cr.aliyuncs.com
`

func TestConfigurationReconcilerAppliesAndFansOut(t *testing.T) {
	t.Parallel()

	options := ControllerOptions{}.withDefaults()
	store := &config.Store{}
	kubernetesClient := fake.NewClientBuilder().
		WithScheme(testScheme(t)).
		WithObjects(
			testConfigMap("production", "default"),
			&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "production"}},
			&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "ignored"}},
			&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: "production"}},
			&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: "other", Namespace: "ignored"}},
		).
		Build()
	publisher := mustResourceEventPublisher(t, kubernetesClient)
	observer := &recordingConfigurationObserver{}
	reconciler := &ConfigurationReconciler{
		client:    kubernetesClient,
		store:     store,
		observer:  observer,
		publisher: publisher,
		options:   options,
	}

	request := ctrl.Request{NamespacedName: types.NamespacedName{
		Namespace: options.ControllerNamespace,
		Name:      options.ConfigMapName,
	}}
	if _, err := reconciler.Reconcile(testContext(), request); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}

	snapshot, loaded := store.Load()
	if !loaded || snapshot.Generation != 1 || !snapshot.MatchesNamespace("production") {
		t.Fatalf("loaded snapshot = %+v, present = %v; want generation 1", snapshot, loaded)
	}
	assertNamespaceEvents(t, publisher.namespaceEvents, "ignored", "production")
	assertServiceAccountEvents(t, publisher.serviceAccountEvents, "ignored/other", "production/default")

	configMap := &corev1.ConfigMap{}
	if err := kubernetesClient.Get(context.Background(), request.NamespacedName, configMap); err != nil {
		t.Fatalf("Get() ConfigMap error = %v", err)
	}
	configMap.Data[config.NamespaceKey] = " production "
	if err := kubernetesClient.Update(context.Background(), configMap); err != nil {
		t.Fatalf("Update() ConfigMap error = %v", err)
	}
	if _, err := reconciler.Reconcile(testContext(), request); err != nil {
		t.Fatalf("equivalent Reconcile() error = %v", err)
	}

	unchanged, _ := store.Load()
	if unchanged.Generation != 1 {
		t.Fatalf("equivalent configuration generation = %d, want 1", unchanged.Generation)
	}
	assertNamespaceEvents(t, publisher.namespaceEvents, "ignored", "production")
	assertServiceAccountEvents(t, publisher.serviceAccountEvents, "ignored/other", "production/default")
	if observer.count() != 2 {
		t.Fatalf("configuration notifications = %d, want 2", observer.count())
	}
}

func TestConfigurationReconcilerRetainsLastValidConfiguration(t *testing.T) {
	t.Parallel()

	options := ControllerOptions{}.withDefaults()
	store := &config.Store{}
	configMap := testConfigMap("production", "default")
	kubernetesClient := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(
		configMap,
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "production"}},
		&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: "production"}},
	).Build()
	publisher := mustResourceEventPublisher(t, kubernetesClient)
	observer := &recordingConfigurationObserver{}
	reconciler := &ConfigurationReconciler{
		client:    kubernetesClient,
		store:     store,
		observer:  observer,
		publisher: publisher,
		options:   options,
	}
	request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(configMap)}

	if _, err := reconciler.Reconcile(testContext(), request); err != nil {
		t.Fatalf("initial Reconcile() error = %v", err)
	}
	assertNamespaceEvents(t, publisher.namespaceEvents, "production")
	assertServiceAccountEvents(t, publisher.serviceAccountEvents, "production/default")

	current := &corev1.ConfigMap{}
	if err := kubernetesClient.Get(context.Background(), request.NamespacedName, current); err != nil {
		t.Fatalf("Get() ConfigMap error = %v", err)
	}
	current.Data[config.NamespaceKey] = "production,,staging"
	if err := kubernetesClient.Update(context.Background(), current); err != nil {
		t.Fatalf("Update() invalid ConfigMap error = %v", err)
	}
	if _, err := reconciler.Reconcile(testContext(), request); err != nil {
		t.Fatalf("invalid Reconcile() error = %v", err)
	}
	assertNoEvent(t, publisher.namespaceEvents, "Namespace")
	assertNoEvent(t, publisher.serviceAccountEvents, "ServiceAccount")

	if err := kubernetesClient.Delete(context.Background(), current); err != nil {
		t.Fatalf("Delete() ConfigMap error = %v", err)
	}
	if _, err := reconciler.Reconcile(testContext(), request); err != nil {
		t.Fatalf("deleted Reconcile() error = %v", err)
	}
	assertNoEvent(t, publisher.namespaceEvents, "Namespace")
	assertNoEvent(t, publisher.serviceAccountEvents, "ServiceAccount")

	after, loaded := store.Load()
	if !loaded || after.Generation != 1 || !after.MatchesNamespace("production") {
		t.Fatalf("snapshot after invalid update and deletion = %+v, present = %v", after, loaded)
	}
	if observer.count() != 1 {
		t.Fatalf("configuration notifications = %d, want only the initial valid configuration", observer.count())
	}
}

func TestConfigurationReconcilerRetriesFanOutAfterStoreApply(t *testing.T) {
	t.Parallel()

	options := ControllerOptions{}.withDefaults()
	store := &config.Store{}
	baseClient := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(
		testConfigMap("production", "default"),
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "production"}},
	).Build()
	kubernetesClient := &failOnceNamespaceListClient{Client: baseClient}
	publisher := mustResourceEventPublisher(t, kubernetesClient)
	reconciler := &ConfigurationReconciler{
		client:    kubernetesClient,
		store:     store,
		observer:  &recordingConfigurationObserver{},
		publisher: publisher,
		options:   options,
	}
	request := ctrl.Request{NamespacedName: types.NamespacedName{
		Namespace: options.ControllerNamespace,
		Name:      options.ConfigMapName,
	}}

	if _, err := reconciler.Reconcile(testContext(), request); err == nil {
		t.Fatal("first Reconcile() error = nil, want Namespace list failure")
	}
	first, loaded := store.Load()
	if !loaded || first.Generation != 1 {
		t.Fatalf("snapshot after failed fan-out = %+v, present = %v; want applied generation 1", first, loaded)
	}

	if _, err := reconciler.Reconcile(testContext(), request); err != nil {
		t.Fatalf("retry Reconcile() error = %v", err)
	}
	second, _ := store.Load()
	if second.Generation != 1 {
		t.Fatalf("retry advanced generation to %d, want 1", second.Generation)
	}
	assertNamespaceEvents(t, publisher.namespaceEvents, "production")
}

func TestNamespaceSecretReconcilerUsesLatestConfigurationGateAndReturnsErrors(t *testing.T) {
	t.Parallel()

	store := &config.Store{}
	retryErr := errors.New("temporary sync failure")
	recorder := newRecordingSyncer(func(call int, namespace string) error {
		if call == 1 && namespace == "production" {
			return retryErr
		}
		return nil
	})
	reconciler := &NamespaceSecretReconciler{store: store, syncer: recorder}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Name: "production"}}

	if _, err := reconciler.Reconcile(testContext(), request); err != nil {
		t.Fatalf("Reconcile() before configuration error = %v", err)
	}
	if recorder.count("production") != 0 {
		t.Fatal("NamespaceSecretSyncer ran before a valid configuration was loaded")
	}

	snapshot, err := config.Parse(testConfigData("production", "default"))
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	store.Apply(snapshot)

	if _, err := reconciler.Reconcile(testContext(), request); !errors.Is(err, retryErr) {
		t.Fatalf("first configured Reconcile() error = %v, want %v", err, retryErr)
	}
	if _, err := reconciler.Reconcile(testContext(), request); err != nil {
		t.Fatalf("second configured Reconcile() error = %v", err)
	}
	if recorder.count("production") != 2 || recorder.successes("production") != 1 {
		t.Fatalf("sync calls = %d, successes = %d; want 2 and 1", recorder.count("production"), recorder.successes("production"))
	}

	namespacedRequest := ctrl.Request{NamespacedName: types.NamespacedName{
		Namespace: "unexpected",
		Name:      "production",
	}}
	if _, err := reconciler.Reconcile(testContext(), namespacedRequest); err != nil {
		t.Fatalf("namespaced Reconcile() error = %v", err)
	}
	if recorder.count("production") != 2 {
		t.Fatal("NamespaceSecretReconciler accepted a namespaced request for a cluster-scoped key")
	}
}

func TestServiceAccountReconcilerPreservesObjectKeyAndReturnsErrors(t *testing.T) {
	t.Parallel()

	store := &config.Store{}
	retryErr := errors.New("temporary ServiceAccount sync failure")
	recorder := newRecordingSyncer(func(call int, key string) error {
		if call == 1 && key == "production/build" {
			return retryErr
		}
		return nil
	})
	reconciler := &ServiceAccountReconciler{store: store, syncer: recorder}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "production", Name: "build"}}

	if _, err := reconciler.Reconcile(testContext(), request); err != nil {
		t.Fatalf("Reconcile() before configuration error = %v", err)
	}
	if recorder.count("production/build") != 0 {
		t.Fatal("ServiceAccountSyncer ran before a valid configuration was loaded")
	}

	snapshot, err := config.Parse(testConfigData("production", "build"))
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	store.Apply(snapshot)

	if _, err := reconciler.Reconcile(testContext(), request); !errors.Is(err, retryErr) {
		t.Fatalf("first configured Reconcile() error = %v, want %v", err, retryErr)
	}
	if _, err := reconciler.Reconcile(testContext(), request); err != nil {
		t.Fatalf("second configured Reconcile() error = %v", err)
	}
	if recorder.count("production/build") != 2 || recorder.successes("production/build") != 1 {
		t.Fatalf(
			"sync calls = %d, successes = %d; want 2 and 1",
			recorder.count("production/build"),
			recorder.successes("production/build"),
		)
	}

	clusterScopedRequest := ctrl.Request{NamespacedName: types.NamespacedName{Name: "build"}}
	if _, err := reconciler.Reconcile(testContext(), clusterScopedRequest); err != nil {
		t.Fatalf("cluster-scoped Reconcile() error = %v", err)
	}
	if recorder.count("production/build") != 2 {
		t.Fatal("ServiceAccountReconciler accepted a cluster-scoped request")
	}

	nonTargetRequest := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "production", Name: "other"}}
	if _, err := reconciler.Reconcile(testContext(), nonTargetRequest); err != nil {
		t.Fatalf("non-target Reconcile() error = %v", err)
	}
	if recorder.count("production/other") != 1 {
		t.Fatal("ServiceAccountReconciler filtered a non-target object needed for cleanup")
	}
}

func TestEventMappersKeepNamespaceAndServiceAccountKeysSeparate(t *testing.T) {
	t.Parallel()

	store := &config.Store{}
	data := testConfigData(config.Wildcard, "default,build")
	data[config.ExcludeNamespaceKey] = "ignored"
	snapshot, err := config.Parse(data)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	store.Apply(snapshot)

	namespaceRequests := mapManagedSecretToNamespace(DefaultManagedSecretName)
	assertNamespaceRequests(t, namespaceRequests(context.Background(), &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: DefaultManagedSecretName, Namespace: "production"},
	}), "production")
	assertNamespaceRequests(t, namespaceRequests(context.Background(), &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "other", Namespace: "production"},
	}))

	kubernetesClient := fake.NewClientBuilder().
		WithScheme(testScheme(t)).
		WithObjects(
			&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: "production"}},
			&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: "build", Namespace: "production"}},
			&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: "other", Namespace: "production"}},
			&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: "build", Namespace: "ignored"}},
		).
		Build()
	serviceAccountRequests := mapManagedSecretToServiceAccounts(
		kubernetesClient,
		store,
		DefaultManagedSecretName,
	)
	assertNamespacedRequests(t, serviceAccountRequests(context.Background(), &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: DefaultManagedSecretName, Namespace: "production"},
	}), "production/build", "production/default")
	assertNamespacedRequests(t, serviceAccountRequests(context.Background(), &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "other", Namespace: "production"},
	}))

	secretEvents := serviceAccountSecretPredicate()
	managedSecret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{
		Name:      DefaultManagedSecretName,
		Namespace: "production",
		Labels: map[string]string{
			registrysecret.ApplicationNameLabelKey: registrysecret.ControllerIdentity,
			registrysecret.ManagedByLabelKey:       registrysecret.ControllerIdentity,
		},
	}}
	unmanagedSecret := managedSecret.DeepCopy()
	delete(unmanagedSecret.Labels, registrysecret.ManagedByLabelKey)
	rotatedSecret := managedSecret.DeepCopy()
	rotatedSecret.Data = map[string][]byte{corev1.DockerConfigJsonKey: []byte("rotated")}

	if !secretEvents.Create(event.CreateEvent{Object: managedSecret}) {
		t.Fatal("managed Secret predicate rejected create")
	}
	if secretEvents.Update(event.UpdateEvent{ObjectOld: managedSecret, ObjectNew: rotatedSecret}) {
		t.Fatal("managed Secret predicate admitted ordinary data update")
	}
	if !secretEvents.Update(event.UpdateEvent{ObjectOld: managedSecret, ObjectNew: unmanagedSecret}) ||
		!secretEvents.Update(event.UpdateEvent{ObjectOld: unmanagedSecret, ObjectNew: managedSecret}) {
		t.Fatal("managed Secret predicate rejected ownership transition")
	}
	if secretEvents.Delete(event.DeleteEvent{Object: managedSecret}) {
		t.Fatal("managed Secret predicate admitted delete")
	}
}

func TestCacheOptionsAndReadiness(t *testing.T) {
	t.Parallel()

	cacheOptions, err := NewCacheOptions(ControllerOptions{})
	if err != nil {
		t.Fatalf("NewCacheOptions() error = %v", err)
	}
	if !cacheOptions.ReaderFailOnMissingInformer || cacheOptions.DefaultTransform == nil {
		t.Fatalf("cache options = %+v; want strict readers and managedFields transform", cacheOptions)
	}

	var configMaps, namespaces, serviceAccounts, secrets bool
	for object, byObject := range cacheOptions.ByObject {
		switch object.(type) {
		case *corev1.ConfigMap:
			configMaps = len(byObject.Namespaces) == 1 &&
				byObject.Field.String() == "metadata.name="+DefaultConfigMapName
		case *corev1.Namespace:
			namespaces = true
		case *corev1.ServiceAccount:
			serviceAccounts = true
		case *corev1.Secret:
			secrets = byObject.Field.String() == "metadata.name="+DefaultManagedSecretName
		}
	}
	if !configMaps || !namespaces || !serviceAccounts || !secrets {
		t.Fatalf("cache registrations: ConfigMap=%v Namespace=%v ServiceAccount=%v Secret=%v", configMaps, namespaces, serviceAccounts, secrets)
	}

	if _, err := NewCacheOptions(ControllerOptions{MaxConcurrentNamespaceReconciles: -1}); err == nil {
		t.Fatal("NewCacheOptions() accepted negative namespace reconcile concurrency")
	}
	if _, err := NewCacheOptions(ControllerOptions{MaxConcurrentServiceAccountReconciles: -1}); err == nil {
		t.Fatal("NewCacheOptions() accepted negative ServiceAccount reconcile concurrency")
	}

	store := &config.Store{}
	check := ConfigurationReadyCheck(store)
	if err := check(nil); err == nil {
		t.Fatal("readiness before valid configuration = nil, want error")
	}
	snapshot, err := config.Parse(testConfigData("production", "default"))
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	store.Apply(snapshot)
	if err := check(nil); err != nil {
		t.Fatalf("readiness after valid configuration error = %v", err)
	}
}

type failOnceNamespaceListClient struct {
	client.Client
	failed bool
}

func (c *failOnceNamespaceListClient) List(ctx context.Context, list client.ObjectList, options ...client.ListOption) error {
	if _, ok := list.(*corev1.NamespaceList); ok && !c.failed {
		c.failed = true
		return errors.New("temporary Namespace list failure")
	}
	return c.Client.List(ctx, list, options...)
}

type recordingConfigurationObserver struct {
	mu            sync.Mutex
	notifications int
}

func (o *recordingConfigurationObserver) NotifyConfigurationChanged() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.notifications++
}

func (o *recordingConfigurationObserver) count() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.notifications
}

func mustResourceEventPublisher(t *testing.T, reader client.Reader) *ResourceEventPublisher {
	t.Helper()
	publisher, err := NewResourceEventPublisher(reader)
	if err != nil {
		t.Fatalf("NewResourceEventPublisher() error = %v", err)
	}
	return publisher
}

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme() error = %v", err)
	}
	return scheme
}

func testContext() context.Context {
	return log.IntoContext(context.Background(), logr.Discard())
}

func testConfigMap(namespace, serviceAccount string) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      DefaultConfigMapName,
			Namespace: DefaultControllerNamespace,
		},
		Data: testConfigData(namespace, serviceAccount),
	}
}

func testConfigData(namespace, serviceAccount string) map[string]string {
	return map[string]string{
		config.NamespaceKey:      namespace,
		config.ServiceAccountKey: serviceAccount,
		config.RegistriesKey:     testRegistries,
	}
}

func assertNamespaceEvents(t *testing.T, events <-chan event.GenericEvent, expected ...string) {
	t.Helper()
	actual := make(map[string]bool, len(expected))
	for range expected {
		select {
		case received := <-events:
			actual[received.Object.GetName()] = true
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for Namespace events; got %v, want %v", actual, expected)
		}
	}
	for _, namespace := range expected {
		if !actual[namespace] {
			t.Fatalf("Namespace events = %v, missing %q", actual, namespace)
		}
	}
}

func assertServiceAccountEvents(t *testing.T, events <-chan event.GenericEvent, expected ...string) {
	t.Helper()
	actual := make(map[string]bool, len(expected))
	for range expected {
		select {
		case received := <-events:
			key := received.Object.GetNamespace() + "/" + received.Object.GetName()
			actual[key] = true
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for ServiceAccount events; got %v, want %v", actual, expected)
		}
	}
	for _, key := range expected {
		if !actual[key] {
			t.Fatalf("ServiceAccount events = %v, missing %q", actual, key)
		}
	}
}

func assertNoEvent(t *testing.T, events <-chan event.GenericEvent, kind string) {
	t.Helper()
	select {
	case received := <-events:
		t.Fatalf("unexpected %s event for %s/%s", kind, received.Object.GetNamespace(), received.Object.GetName())
	default:
	}
}

func assertNamespaceRequests(t *testing.T, actual []reconcile.Request, expected ...string) {
	t.Helper()
	if len(actual) != len(expected) {
		t.Fatalf("requests = %+v, want Names %v", actual, expected)
	}
	for index, namespace := range expected {
		if actual[index].Name != namespace || actual[index].Namespace != "" {
			t.Fatalf("request[%d] = %+v, want cluster-scoped key %q", index, actual[index], namespace)
		}
	}
}

func assertNamespacedRequests(t *testing.T, actual []reconcile.Request, expected ...string) {
	t.Helper()
	if len(actual) != len(expected) {
		t.Fatalf("requests = %+v, want keys %v", actual, expected)
	}
	want := make(map[string]bool, len(expected))
	for _, key := range expected {
		want[key] = true
	}
	for _, request := range actual {
		key := request.Namespace + "/" + request.Name
		if !want[key] {
			t.Fatalf("unexpected request %+v; want keys %v", request, expected)
		}
	}
}

type recordingSyncer struct {
	mu         sync.Mutex
	calls      map[string]int
	successful map[string]int
	notify     chan struct{}
	result     func(call int, namespace string) error
}

func newRecordingSyncer(result func(call int, namespace string) error) *recordingSyncer {
	return &recordingSyncer{
		calls:      make(map[string]int),
		successful: make(map[string]int),
		notify:     make(chan struct{}, 1),
		result:     result,
	}
}

func (r *recordingSyncer) SyncNamespaceSecret(_ context.Context, namespace string) error {
	return r.record(namespace)
}

func (r *recordingSyncer) SyncServiceAccount(_ context.Context, key types.NamespacedName) error {
	return r.record(key.Namespace + "/" + key.Name)
}

func (r *recordingSyncer) record(key string) error {
	r.mu.Lock()
	r.calls[key]++
	call := r.calls[key]
	result := r.result
	r.mu.Unlock()

	var err error
	if result != nil {
		err = result(call, key)
	}
	if err == nil {
		r.mu.Lock()
		r.successful[key]++
		r.mu.Unlock()
	}
	select {
	case r.notify <- struct{}{}:
	default:
	}
	return err
}

func (r *recordingSyncer) count(key string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls[key]
}

func (r *recordingSyncer) successes(key string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.successful[key]
}

func (r *recordingSyncer) waitForCount(t *testing.T, key string, count int) {
	t.Helper()
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	for {
		if r.count(key) >= count {
			return
		}
		select {
		case <-r.notify:
		case <-deadline.C:
			t.Fatalf("key %q call count = %d, want at least %d", key, r.count(key), count)
		}
	}
}
