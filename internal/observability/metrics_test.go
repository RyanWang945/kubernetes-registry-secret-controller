package observability

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/RyanWang945/kubernetes-registry-secret-controller/internal/config"
	"github.com/RyanWang945/kubernetes-registry-secret-controller/internal/credential"
	"github.com/RyanWang945/kubernetes-registry-secret-controller/internal/registrysecret"
)

func fixture(t testing.TB) (config.RegistryConfig, *config.Store, *credential.Store, time.Time) {
	t.Helper()
	r := config.RegistryConfig{RegionID: "cn-test", InstanceID: "cri-a", AccessKeyID: "sensitive-key", AccessKeySecret: "sensitive-secret", Domains: []string{"registry.example.com"}}
	cfg := &config.Store{}
	cfg.Apply(config.ConfigurationSnapshot{Namespaces: config.NameSelector{MatchAll: true}, Registries: map[config.RegistryKey]config.RegistryConfig{r.Key(): r}})
	now := time.Unix(2_000_000_000, 0).UTC()
	creds := &credential.Store{}
	creds.Apply(r, credential.Credential{Username: "sensitive-user", Password: "sensitive-token", RefreshedAt: now, ExpiresAt: now.Add(time.Hour)})
	return r, cfg, creds, now
}

func rendered(t testing.TB, name string, cfg *config.Store, creds *credential.Store) *corev1.Secret {
	t.Helper()
	snapshot, _ := cfg.Load()
	content, err := registrysecret.Build(snapshot, creds.Snapshot(), nil)
	if err != nil {
		t.Fatal(err)
	}
	return &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "auto-patch-secret", Namespace: name, Labels: map[string]string{registrysecret.ApplicationNameLabelKey: registrysecret.ControllerIdentity, registrysecret.ManagedByLabelKey: registrysecret.ControllerIdentity}, Annotations: map[string]string{registrysecret.StateAnnotationKey: content.StateJSON}}, Type: corev1.SecretTypeDockerConfigJson, Data: map[string][]byte{corev1.DockerConfigJsonKey: content.DockerJSON}}
}

func fixtureMetrics(t testing.TB, cfg *config.Store, creds *credential.Store, now time.Time, objects ...client.Object) (*Metrics, *prometheus.Registry, client.Client) {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
	reg := prometheus.NewRegistry()
	m, err := New(reg, cfg, creds, Options{Reader: kube, WaitForCacheSync: func(context.Context) bool { return true }, SecretName: "auto-patch-secret", Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	m.active.Store(true)
	m.synced.Store(true)
	return m, reg, kube
}

func gauge(t testing.TB, reg *prometheus.Registry, name string, labels map[string]string) (float64, bool) {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() != prefix+name {
			continue
		}
		for _, metric := range family.Metric {
			matches := true
			for key, value := range labels {
				found := false
				for _, label := range metric.Label {
					if label.GetName() == key && label.GetValue() == value {
						found = true
					}
				}
				matches = matches && found
			}
			if matches {
				return metric.GetGauge().GetValue(), true
			}
		}
	}
	return 0, false
}

func TestCollectorTracksCopiesIndependentlyFromMemory(t *testing.T) {
	r, cfg, creds, now := fixture(t)
	objects := []client.Object{}
	for _, name := range []string{"synced", "stale", "expired", "missing", "invalid", "conflict", "deleting"} {
		objects = append(objects, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}})
	}
	synced := rendered(t, "synced", cfg, creds)
	staleCreds := &credential.Store{}
	staleCreds.Apply(r, credential.Credential{Username: "old", Password: "old", RefreshedAt: now.Add(-time.Hour), ExpiresAt: now.Add(time.Minute)})
	stale := rendered(t, "stale", cfg, staleCreds)
	expiredCreds := &credential.Store{}
	expiredCreds.Apply(r, credential.Credential{Username: "old", Password: "old", RefreshedAt: now.Add(-2 * time.Hour), ExpiresAt: now.Add(-time.Minute)})
	expired := rendered(t, "expired", cfg, expiredCreds)
	invalid := rendered(t, "invalid", cfg, creds)
	invalid.Data[corev1.DockerConfigJsonKey] = []byte(`{"auths":{"registry.example.com":{"password":"sensitive-token"}}}`)
	conflict := rendered(t, "conflict", cfg, creds)
	conflict.Labels = nil
	deleting := rendered(t, "deleting", cfg, creds)
	ts := metav1.NewTime(now)
	deleting.DeletionTimestamp = &ts
	deleting.Finalizers = []string{"test/finalizer"}
	objects = append(objects, synced, stale, expired, invalid, conflict, deleting)
	_, reg, _ := fixtureMetrics(t, cfg, creds, now, objects...)
	for state, want := range map[string]float64{"synced": 1, "pending": 3, "expired": 1, "invalid": 1, "conflict": 1} {
		if got, ok := gauge(t, reg, "secret_targets", map[string]string{"registry": r.Key().String(), "state": state}); !ok || got != want {
			t.Fatalf("state %s = %v/%v, want %v", state, got, ok, want)
		}
	}
	if got, _ := gauge(t, reg, "credential_expiration_timestamp_seconds", nil); got != float64(now.Add(time.Hour).Unix()) {
		t.Fatal("memory expiration is incorrect")
	}
	if got, _ := gauge(t, reg, "oldest_secret_expiration_timestamp_seconds", nil); got != float64(now.Add(-time.Minute).Unix()) {
		t.Fatal("old copy expiration was hidden by successful acquisition")
	}
	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		for _, metric := range family.Metric {
			for _, label := range metric.Label {
				if label.GetName() != "registry" && label.GetName() != "state" && label.GetName() != "result" {
					t.Fatalf("unexpected label: %s", label.GetName())
				}
				if strings.Contains(label.GetValue(), "sensitive") {
					t.Fatal("sensitive data in metric labels")
				}
			}
		}
	}
}

type failingReader struct{ client.Reader }

func (r failingReader) List(context.Context, client.ObjectList, ...client.ListOption) error {
	return errors.New("cache read failed")
}

func TestCollectorLifecycleAndFailureDoNotReportFalseHealth(t *testing.T) {
	r, cfg, creds, now := fixture(t)
	m, reg, kube := fixtureMetrics(t, cfg, creds, now, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "missing"}})
	if got, _ := gauge(t, reg, "secret_targets", map[string]string{"state": "pending"}); got != 1 {
		t.Fatal("missing copy was not reported")
	}
	m.options.Reader = failingReader{kube}
	if got, _ := gauge(t, reg, "observation_success", nil); got != 0 {
		t.Fatal("failed observation reported success")
	}
	if _, ok := gauge(t, reg, "secret_targets", nil); ok {
		t.Fatal("failed observation emitted state")
	}
	m.options.Reader = kube
	m.synced.Store(false)
	if _, ok := gauge(t, reg, "secret_targets", nil); ok {
		t.Fatal("unsynchronized cache emitted state")
	}
	m.synced.Store(true)
	m.active.Store(false)
	if _, ok := gauge(t, reg, "credential_expiration_timestamp_seconds", nil); ok {
		t.Fatal("follower emitted business state")
	}
	m.active.Store(true)
	cfg.MarkInvalid()
	if got, _ := gauge(t, reg, "config_valid", nil); got != 0 || !cfg.Loaded() {
		t.Fatal("invalid update did not preserve last good state")
	}
	creds.Delete(r.Key())
	if _, ok := gauge(t, reg, "credential_expiration_timestamp_seconds", nil); ok {
		t.Fatal("missing credentials emitted an expiration")
	}
	snapshot, _ := cfg.Load()
	snapshot.Registries = nil
	cfg.Apply(snapshot)
	if _, ok := gauge(t, reg, "secret_targets", nil); ok {
		t.Fatal("removed registry left stale target metrics")
	}
	families, _ := reg.Gather()
	for _, family := range families {
		if family.GetName() == prefix+"provider_requests_total" {
			t.Fatal("removed registry left request labels")
		}
	}
}

type providerFunc func(context.Context, credential.Request) (credential.Token, error)

func (f providerFunc) GetAuthorizationToken(ctx context.Context, r credential.Request) (credential.Token, error) {
	return f(ctx, r)
}

func TestMeasuredProviderAndConcurrentScrapes(t *testing.T) {
	r, cfg, creds, now := fixture(t)
	m, reg, _ := fixtureMetrics(t, cfg, creds, now)
	request := credential.Request{Key: r.Key()}
	provider := m.WrapProvider(providerFunc(func(context.Context, credential.Request) (credential.Token, error) { return credential.Token{}, nil }))
	var wg sync.WaitGroup
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = provider.GetAuthorizationToken(context.Background(), request)
			_, _ = reg.Gather()
		}()
	}
	wg.Wait()
	failure := m.WrapProvider(providerFunc(func(context.Context, credential.Request) (credential.Token, error) {
		return credential.Token{}, context.DeadlineExceeded
	}))
	_, _ = failure.GetAuthorizationToken(context.Background(), request)
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	_, _ = failure.GetAuthorizationToken(canceled, request)
	if got := testutil.ToFloat64(m.requests.WithLabelValues(r.Key().String(), "success")); got != 20 {
		t.Fatalf("success count=%v", got)
	}
	if got := testutil.ToFloat64(m.requests.WithLabelValues(r.Key().String(), "error")); got != 1 {
		t.Fatalf("error count=%v", got)
	}
}

func TestMetricsRunnableStopsPublishingAfterCancellation(t *testing.T) {
	_, cfg, creds, now := fixture(t)
	m, reg, _ := fixtureMetrics(t, cfg, creds, now)
	m.active.Store(false)
	m.synced.Store(false)
	ready := make(chan struct{})
	m.options.WaitForCacheSync = func(context.Context) bool { close(ready); return true }
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- m.Start(ctx) }()
	<-ready
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if got, _ := gauge(t, reg, "active", nil); got != 0 {
		t.Fatal("stopped leader still active")
	}
	if _, ok := gauge(t, reg, "secret_targets", nil); ok {
		t.Fatal("stopped leader still publishes business state")
	}
}

// A plain in-memory reader models the informer cache; no API server, provider or
// network is involved. Include realistic deep copies performed by cache.List.
type snapshotReader struct {
	client.Reader
	namespaces corev1.NamespaceList
	secrets    corev1.SecretList
}

func (r *snapshotReader) List(_ context.Context, list client.ObjectList, _ ...client.ListOption) error {
	switch typed := list.(type) {
	case *corev1.NamespaceList:
		*typed = *r.namespaces.DeepCopy()
	case *corev1.SecretList:
		*typed = *r.secrets.DeepCopy()
	default:
		return errors.New("unexpected list")
	}
	return nil
}

func BenchmarkCollect5000Namespaces(b *testing.B) {
	_, cfg, creds, now := fixture(b)
	m, reg, _ := fixtureMetrics(b, cfg, creds, now)
	reader := &snapshotReader{}
	for index := range 5000 {
		name := fmt.Sprintf("ns-%d", index)
		reader.namespaces.Items = append(reader.namespaces.Items, corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}})
		reader.secrets.Items = append(reader.secrets.Items, *rendered(b, name, cfg, creds))
	}
	m.options.Reader = reader
	b.ReportAllocs()
	b.ResetTimer()
	samples := make([]time.Duration, 0, b.N)
	for range b.N {
		start := time.Now()
		if _, err := reg.Gather(); err != nil {
			b.Fatal(err)
		}
		samples = append(samples, time.Since(start))
	}
	b.StopTimer()
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	p99 := samples[(len(samples)-1)*99/100]
	b.ReportMetric(p99.Seconds(), "p99-seconds")
	if p99 >= 5*time.Second {
		b.Fatal("local cache collection exceeds the 5-second scrape timeout")
	}
}
