package controller

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/RyanWang945/kubernetes-registry-secret-controller/internal/config"
)

const testRegistries = `
- regionID: cn-hangzhou
  instanceID: cri-test
  accessKeyID: key-test
  accessKeySecret: secret-test
  domains:
    - registry-test.cn-hangzhou.cr.aliyuncs.com
`

func TestEventHandlersMapResourcesToNamespaceKeys(t *testing.T) {
	t.Parallel()

	client := fake.NewSimpleClientset()
	store := &config.Store{}
	parser, err := config.NewParser(DefaultControllerNamespace)
	if err != nil {
		t.Fatalf("NewParser() error = %v", err)
	}
	snapshot, err := parser.Parse(testConfigData("production", "default,build"))
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	store.Apply(snapshot)

	resourceController := newTestController(t, client, store, NamespaceSyncFunc(func(context.Context, string) error {
		return nil
	}))
	t.Cleanup(resourceController.resourceQueue.ShutDown)

	resourceController.onNamespaceAdd(&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "ignored"}})
	resourceController.onNamespaceAdd(&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "production"}})
	resourceController.onNamespaceAdd(&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "production"}})
	if got := resourceController.resourceQueue.Len(); got != 1 {
		t.Fatalf("queue length after Namespace events = %d, want 1", got)
	}
	if key := takeQueueKey(t, resourceController); key != "production" {
		t.Fatalf("Namespace event key = %q, want production", key)
	}

	resourceController.onServiceAccountChange(&corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{Name: "ignored", Namespace: "production"},
	})
	if got := resourceController.resourceQueue.Len(); got != 0 {
		t.Fatalf("queue length after non-target ServiceAccount = %d, want 0", got)
	}

	resourceController.onServiceAccountChange(&corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{Name: "build", Namespace: "production"},
	})
	if key := takeQueueKey(t, resourceController); key != "production" {
		t.Fatalf("ServiceAccount event key = %q, want production", key)
	}
}

func TestConfigMapUpdatesAreAtomicAndDeletionRetainsConfiguration(t *testing.T) {
	t.Parallel()

	client := fake.NewSimpleClientset()
	store := &config.Store{}
	resourceController := newTestController(t, client, store, NamespaceSyncFunc(func(context.Context, string) error {
		return nil
	}))
	t.Cleanup(resourceController.resourceQueue.ShutDown)

	valid := testConfigMap("production", "default")
	resourceController.onConfigMapAdd(valid)
	first, loaded := store.Load()
	if !loaded || first.Generation != 1 {
		t.Fatalf("first snapshot = %+v, loaded = %v; want generation 1", first, loaded)
	}

	equivalent := valid.DeepCopy()
	equivalent.Data[config.NamespaceKey] = " production "
	resourceController.onConfigMapAdd(equivalent)
	unchanged, _ := store.Load()
	if unchanged.Generation != 1 {
		t.Fatalf("equivalent update generation = %d, want 1", unchanged.Generation)
	}

	invalid := valid.DeepCopy()
	invalid.Data[config.NamespaceKey] = "production,,staging"
	resourceController.onConfigMapAdd(invalid)
	afterInvalid, _ := store.Load()
	if afterInvalid.Generation != 1 || !afterInvalid.Equal(first) {
		t.Fatalf("invalid update replaced the last valid configuration: %+v", afterInvalid)
	}

	resourceController.onConfigMapDelete(valid)
	afterDelete, _ := store.Load()
	if afterDelete.Generation != 1 || !afterDelete.Equal(first) {
		t.Fatalf("delete removed the last valid configuration: %+v", afterDelete)
	}

	changed := valid.DeepCopy()
	changed.Data[config.NamespaceKey] = "production,staging"
	resourceController.onConfigMapAdd(changed)
	second, _ := store.Load()
	if second.Generation != 2 || !second.Namespaces.Matches("staging") {
		t.Fatalf("valid update snapshot = %+v, want generation 2 targeting staging", second)
	}
}

func TestControllerInitialListAndContinuousWatch(t *testing.T) {
	t.Parallel()

	client := fake.NewSimpleClientset(
		testConfigMap("production,staging", "default,build"),
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "production"}},
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "ignored"}},
	)
	recorder := newRecordingSyncer(nil)
	store := &config.Store{}
	resourceController := newTestController(t, client, store, recorder)

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- resourceController.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-runDone:
			if err != nil {
				t.Errorf("Run() error = %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("Run() did not stop within 5 seconds")
		}
	})

	waitForSignal(t, resourceController.CachesSynced(), "informer cache synchronization")
	recorder.waitForCount(t, "production", 1)
	if recorder.count("ignored") != 0 {
		t.Fatalf("ignored namespace reconciliations = %d, want 0", recorder.count("ignored"))
	}

	_, err := client.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: "staging"},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("create target Namespace: %v", err)
	}
	recorder.waitForCount(t, "staging", 1)

	productionBefore := recorder.count("production")
	_, err = client.CoreV1().ServiceAccounts("production").Create(ctx, &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{Name: "build", Namespace: "production"},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("create target ServiceAccount: %v", err)
	}
	recorder.waitForCount(t, "production", productionBefore+1)

	loaded, ok := store.Load()
	if !ok || loaded.Generation != 1 {
		t.Fatalf("loaded configuration = %+v, present = %v; want generation 1", loaded, ok)
	}
}

func TestControllerWaitsForValidConfigAndRetriesSync(t *testing.T) {
	t.Parallel()

	client := fake.NewSimpleClientset(
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "production"}},
	)
	retryErr := errors.New("temporary sync failure")
	recorder := newRecordingSyncer(func(call int, namespace string) error {
		if namespace == "production" && call == 1 {
			return retryErr
		}
		return nil
	})
	store := &config.Store{}
	resourceController := newTestController(t, client, store, recorder)

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- resourceController.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-runDone:
			if err != nil {
				t.Errorf("Run() error = %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("Run() did not stop within 5 seconds")
		}
	})

	waitForSignal(t, resourceController.CachesSynced(), "informer cache synchronization")
	if recorder.count("production") != 0 {
		t.Fatal("resource worker ran before a valid ConfigMap was loaded")
	}

	_, err := client.CoreV1().ConfigMaps(DefaultControllerNamespace).Create(
		ctx,
		testConfigMap("production", "default"),
		metav1.CreateOptions{},
	)
	if err != nil {
		t.Fatalf("create ConfigMap: %v", err)
	}
	recorder.waitForCount(t, "production", 2)
	if recorder.successes("production") != 1 {
		t.Fatalf("successful production reconciliations = %d, want 1", recorder.successes("production"))
	}
}

func TestConfigMapDeleteWatchRetainsLastValidSnapshot(t *testing.T) {
	t.Parallel()

	configMap := testConfigMap("production", "default")
	client := fake.NewSimpleClientset(configMap)
	store := &config.Store{}
	resourceController := newTestController(t, client, store, newRecordingSyncer(nil))

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- resourceController.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-runDone:
			if err != nil {
				t.Errorf("Run() error = %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("Run() did not stop within 5 seconds")
		}
	})

	waitForSignal(t, resourceController.CachesSynced(), "informer cache synchronization")
	eventually(t, 5*time.Second, func() bool {
		snapshot, ok := store.Load()
		return ok && snapshot.Generation == 1
	}, "initial valid configuration was not loaded")

	if err := client.CoreV1().ConfigMaps(DefaultControllerNamespace).Delete(
		ctx,
		DefaultConfigMapName,
		metav1.DeleteOptions{},
	); err != nil {
		t.Fatalf("delete ConfigMap: %v", err)
	}

	eventually(t, 5*time.Second, func() bool {
		_, err := resourceController.configMapInformer.Lister().ConfigMaps(DefaultControllerNamespace).Get(DefaultConfigMapName)
		return apierrors.IsNotFound(err)
	}, "ConfigMap delete was not observed by the informer")

	snapshot, ok := store.Load()
	if !ok || snapshot.Generation != 1 || !snapshot.Namespaces.Matches("production") {
		t.Fatalf("snapshot after ConfigMap deletion = %+v, present = %v", snapshot, ok)
	}
}

func newTestController(
	t *testing.T,
	client *fake.Clientset,
	store *config.Store,
	syncer NamespaceSyncer,
) *Controller {
	t.Helper()
	resourceController, err := New(client, store, syncer, Options{Logger: testLogger()})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return resourceController
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
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

func takeQueueKey(t *testing.T, resourceController *Controller) string {
	t.Helper()
	key, shutdown := resourceController.resourceQueue.Get()
	if shutdown {
		t.Fatal("queue shut down while taking a key")
	}
	resourceController.resourceQueue.Done(key)
	resourceController.resourceQueue.Forget(key)
	return key
}

func waitForSignal(t *testing.T, signal <-chan struct{}, description string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", description)
	}
}

func eventually(t *testing.T, timeout time.Duration, condition func() bool, failureMessage string) {
	t.Helper()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		if condition() {
			return
		}
		select {
		case <-deadline.C:
			t.Fatal(failureMessage)
		case <-ticker.C:
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

func (r *recordingSyncer) SyncNamespace(_ context.Context, namespace string) error {
	r.mu.Lock()
	r.calls[namespace]++
	call := r.calls[namespace]
	result := r.result
	r.mu.Unlock()

	var err error
	if result != nil {
		err = result(call, namespace)
	}

	if err == nil {
		r.mu.Lock()
		r.successful[namespace]++
		r.mu.Unlock()
	}
	select {
	case r.notify <- struct{}{}:
	default:
	}
	return err
}

func (r *recordingSyncer) count(namespace string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls[namespace]
}

func (r *recordingSyncer) successes(namespace string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.successful[namespace]
}

func (r *recordingSyncer) waitForCount(t *testing.T, namespace string, count int) {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for {
		if r.count(namespace) >= count {
			return
		}
		select {
		case <-r.notify:
		case <-deadline.C:
			t.Fatalf("namespace %q call count = %d, want at least %d", namespace, r.count(namespace), count)
		}
	}
}
