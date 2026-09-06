package observability

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/RyanWang945/kubernetes-registry-secret-controller/internal/config"
	"github.com/RyanWang945/kubernetes-registry-secret-controller/internal/credential"
	"github.com/RyanWang945/kubernetes-registry-secret-controller/internal/registrysecret"
)

const prefix = "registry_secret_controller_"

var targetStates = []string{"synced", "pending", "conflict", "invalid", "expired"}

// Options.Reader must be the Manager Cache, never a direct API client. The
// callback prevents List from starting/waiting for informers in a scrape.
type Options struct {
	Reader           client.Reader
	WaitForCacheSync func(context.Context) bool
	SecretName       string
	Now              func() time.Time
}

type Metrics struct {
	config      *config.Store
	credentials *credential.Store
	options     Options
	active      atomic.Bool
	synced      atomic.Bool
	mu          sync.Mutex
	requests    *prometheus.CounterVec
	duration    *prometheus.HistogramVec
	known       map[config.RegistryKey]struct{}

	activeDesc, successDesc, configDesc, expiryDesc, targetsDesc, oldestDesc *prometheus.Desc
}

func New(reg prometheus.Registerer, cfg *config.Store, credentials *credential.Store, options Options) (*Metrics, error) {
	if reg == nil || cfg == nil || credentials == nil || options.Reader == nil || options.WaitForCacheSync == nil || options.SecretName == "" {
		return nil, errors.New("metrics requires a registry, stores, synchronized cache and Secret name")
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	desc := func(name, help string, labels ...string) *prometheus.Desc {
		return prometheus.NewDesc(prefix+name, help, labels, nil)
	}
	m := &Metrics{
		config: cfg, credentials: credentials, options: options, known: make(map[config.RegistryKey]struct{}),
		requests:    prometheus.NewCounterVec(prometheus.CounterOpts{Name: prefix + "provider_requests_total", Help: "Completed Provider calls by registry and result."}, []string{"registry", "result"}),
		duration:    prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: prefix + "provider_request_duration_seconds", Help: "Wall-clock duration of Provider calls.", Buckets: []float64{.05, .1, .25, .5, 1, 2.5, 5, 10, 15, 30}}, []string{"result"}),
		activeDesc:  desc("active", "Whether this process is running leader-only business work."),
		successDesc: desc("observation_success", "Whether business state was observed from synchronized caches."),
		configDesc:  desc("config_valid", "Whether the latest configuration is valid; independent of last-known-good readiness."),
		expiryDesc:  desc("credential_expiration_timestamp_seconds", "Expiration of the current in-memory credential.", "registry"),
		targetsDesc: desc("secret_targets", "Target namespaces by registry and mutually exclusive state.", "registry", "state"),
		oldestDesc:  desc("oldest_secret_expiration_timestamp_seconds", "Earliest observed expiration among structurally usable target copies.", "registry"),
	}
	for _, result := range []string{"success", "error"} {
		m.duration.WithLabelValues(result)
	}
	if err := reg.Register(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (*Metrics) NeedLeaderElection() bool { return true }
func (m *Metrics) Start(ctx context.Context) error {
	m.active.Store(true)
	defer m.active.Store(false)
	defer m.synced.Store(false)
	if m.options.WaitForCacheSync(ctx) {
		m.synced.Store(true)
	}
	<-ctx.Done()
	return nil
}

func (m *Metrics) Describe(ch chan<- *prometheus.Desc) {
	m.requests.Describe(ch)
	m.duration.Describe(ch)
	for _, d := range []*prometheus.Desc{m.activeDesc, m.successDesc, m.configDesc, m.expiryDesc, m.targetsDesc, m.oldestDesc} {
		ch <- d
	}
}

func (m *Metrics) Collect(ch chan<- prometheus.Metric) {
	snapshot, loaded := m.config.Load()
	m.mu.Lock()
	for key := range m.known {
		if _, found := snapshot.Registries[key]; !found {
			m.requests.DeleteLabelValues(key.String(), "success")
			m.requests.DeleteLabelValues(key.String(), "error")
			delete(m.known, key)
		}
	}
	for key := range snapshot.Registries {
		m.known[key] = struct{}{}
		m.requests.WithLabelValues(key.String(), "success")
		m.requests.WithLabelValues(key.String(), "error")
	}
	m.requests.Collect(ch)
	m.mu.Unlock()
	m.duration.Collect(ch)
	emit := func(desc *prometheus.Desc, value float64, labels ...string) {
		ch <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, value, labels...)
	}
	emit(m.activeDesc, boolValue(m.active.Load()))
	emit(m.configDesc, boolValue(m.config.Valid()))
	if !m.active.Load() || !m.synced.Load() || !loaded {
		emit(m.successDesc, 0)
		return
	}
	// Only local cache reads; compute the complete result before emitting any
	// state so a failed List cannot masquerade as zero unhealthy targets.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	namespaces := &corev1.NamespaceList{}
	secrets := &corev1.SecretList{}
	if err := m.options.Reader.List(ctx, namespaces); err != nil {
		emit(m.successDesc, 0)
		return
	}
	if err := m.options.Reader.List(ctx, secrets); err != nil {
		emit(m.successDesc, 0)
		return
	}
	credentials := m.credentials.Snapshot()
	content, err := registrysecret.Build(snapshot, credentials, nil)
	if err != nil {
		emit(m.successDesc, 0)
		return
	}
	expected := registrysecret.Inspect(&corev1.Secret{Data: map[string][]byte{corev1.DockerConfigJsonKey: content.DockerJSON}, ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{registrysecret.StateAnnotationKey: content.StateJSON}}})
	byNamespace := make(map[string]*corev1.Secret, len(secrets.Items))
	for index := range secrets.Items {
		secret := &secrets.Items[index]
		if secret.Name == m.options.SecretName {
			byNamespace[secret.Namespace] = secret
		}
	}
	counts := make(map[config.RegistryKey]map[string]int, len(snapshot.Registries))
	oldest := make(map[config.RegistryKey]time.Time)
	for key := range snapshot.Registries {
		counts[key] = make(map[string]int, len(targetStates))
	}
	now := m.options.Now()
	for _, namespace := range namespaces.Items {
		if namespace.DeletionTimestamp != nil || namespace.Status.Phase == corev1.NamespaceTerminating || !snapshot.MatchesNamespace(namespace.Name) {
			continue
		}
		actual := registrysecret.Inspect(byNamespace[namespace.Name])
		for key, registry := range snapshot.Registries {
			state, expires := actual.RegistryStatus(registry, expected, now)
			counts[key][state]++
			if !expires.IsZero() && (oldest[key].IsZero() || expires.Before(oldest[key])) {
				oldest[key] = expires
			}
		}
	}
	// Leadership can be lost while assembling a large snapshot.
	if !m.active.Load() {
		emit(m.successDesc, 0)
		return
	}
	emit(m.successDesc, 1)
	for key, registry := range snapshot.Registries {
		for _, state := range targetStates {
			emit(m.targetsDesc, float64(counts[key][state]), key.String(), state)
		}
		if entry, ok := credentials[key]; ok && entry.MatchesRegistry(registry) {
			emit(m.expiryDesc, float64(entry.Credential.ExpiresAt.Unix()), key.String())
		}
		if value := oldest[key]; !value.IsZero() {
			emit(m.oldestDesc, float64(value.Unix()), key.String())
		}
	}
}

func boolValue(value bool) float64 {
	if value {
		return 1
	}
	return 0
}

func (m *Metrics) WrapProvider(provider credential.TokenProvider) credential.TokenProvider {
	return &measuredProvider{provider: provider, metrics: m}
}

type measuredProvider struct {
	provider credential.TokenProvider
	metrics  *Metrics
}

func (p *measuredProvider) GetAuthorizationToken(ctx context.Context, request credential.Request) (credential.Token, error) {
	start := time.Now()
	token, err := p.provider.GetAuthorizationToken(ctx, request)
	// Shutdown cancellation is neither an ACR failure nor a completed request.
	if ctx.Err() != nil && errors.Is(ctx.Err(), context.Canceled) {
		return token, err
	}
	result := "success"
	if err != nil {
		result = "error"
	}
	p.metrics.duration.WithLabelValues(result).Observe(time.Since(start).Seconds())
	p.metrics.mu.Lock()
	snapshot, _ := p.metrics.config.Load()
	if _, configured := snapshot.Registries[request.Key]; configured {
		p.metrics.known[request.Key] = struct{}{}
		p.metrics.requests.WithLabelValues(request.Key.String(), result).Inc()
	}
	p.metrics.mu.Unlock()
	return token, err
}
