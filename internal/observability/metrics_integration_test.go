//go:build integration

package observability

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus"
	authenticationv1 "k8s.io/api/authentication/v1"
	authorizationv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	certutil "k8s.io/client-go/util/cert"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/RyanWang945/kubernetes-registry-secret-controller/internal/config"
	"github.com/RyanWang945/kubernetes-registry-secret-controller/internal/controller"
)

// Exercise the actual runtime server and native filter, with a review API stub
// so this test needs no cluster credentials or long-lived bearer tokens.
func TestMetricsEndpointAuthenticationAndContent(t *testing.T) {
	_, cfg, creds, now := fixture(t)
	m, _, _ := fixtureMetrics(t, cfg, creds, now,
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "target"}}, rendered(t, "target", cfg, creds))
	if err := ctrlmetrics.Registry.Register(m); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ctrlmetrics.Registry.Unregister(m) })

	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/apis/authentication.k8s.io/v1/tokenreviews":
			var review authenticationv1.TokenReview
			if err := json.NewDecoder(r.Body).Decode(&review); err != nil {
				http.Error(w, "invalid review", http.StatusBadRequest)
				return
			}
			name := review.Spec.Token
			review.Spec = authenticationv1.TokenReviewSpec{}
			review.Status = authenticationv1.TokenReviewStatus{Authenticated: name == "allowed" || name == "denied", User: authenticationv1.UserInfo{Username: name}}
			_ = json.NewEncoder(w).Encode(review)
		case "/apis/authorization.k8s.io/v1/subjectaccessreviews":
			var review authorizationv1.SubjectAccessReview
			if err := json.NewDecoder(r.Body).Decode(&review); err != nil {
				http.Error(w, "invalid review", http.StatusBadRequest)
				return
			}
			a := review.Spec.NonResourceAttributes
			review.Status.Allowed = review.Spec.User == "allowed" && a != nil && a.Path == "/metrics" && a.Verb == "get"
			_ = json.NewEncoder(w).Encode(review)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(api.Close)
	cert, key, err := certutil.GenerateSelfSignedCertKey("127.0.0.1", []net.IP{net.ParseIP("127.0.0.1")}, nil)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	for name, data := range map[string][]byte{"tls.crt": cert, "tls.key": key} {
		if err := os.WriteFile(filepath.Join(dir, name), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	opts, err := ServerOptions("127.0.0.1:0", true, dir)
	if err != nil {
		t.Fatal(err)
	}
	server, err := metricsserver.NewServer(opts, &rest.Config{Host: api.URL, ContentConfig: rest.ContentConfig{ContentType: "application/json"}}, api.Client())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Start(ctx) }()
	t.Cleanup(func() { stopRunnable(t, cancel, done) })
	address := server.(interface{ GetBindAddr() string })
	await(t, func() bool { return address.GetBindAddr() != "" }, "metrics listener")
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(cert) {
		t.Fatal("cannot load test CA")
	}
	transport := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}}
	t.Cleanup(transport.CloseIdleConnections)
	httpClient := &http.Client{Transport: transport, Timeout: 5 * time.Second}
	for _, tc := range []struct {
		token string
		want  int
	}{{"", 401}, {"denied", 403}, {"allowed", 200}} {
		req, err := http.NewRequest(http.MethodGet, "https://"+address.GetBindAddr()+"/metrics", nil)
		if err != nil {
			t.Fatal(err)
		}
		if tc.token != "" {
			req.Header.Set("Authorization", "Bearer "+tc.token)
		}
		response, err := httpClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(response.Body)
		response.Body.Close()
		if err != nil || response.StatusCode != tc.want {
			t.Fatalf("status = %d, want %d; body = %s; err = %v", response.StatusCode, tc.want, body, err)
		}
		if tc.want == http.StatusOK {
			for _, name := range []string{"go_goroutines", prefix + "secret_targets", prefix + "observation_success 1"} {
				if !strings.Contains(string(body), name) {
					t.Errorf("missing metric %s", name)
				}
			}
			if strings.Contains(string(body), "sensitive") {
				t.Error("metrics exposed a credential")
			}
		}
	}
}

func TestBusinessMetricsFollowManagerLeadership(t *testing.T) {
	environment := &envtest.Environment{DownloadBinaryAssets: true, DownloadBinaryAssetsVersion: "1.36.2", BinaryAssetsDirectory: filepath.Join("..", "..", ".cache", "envtest")}
	restConfig, err := environment.Start()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := environment.Stop(); err != nil {
			t.Error(err)
		}
	})
	kube, err := client.New(restConfig, client.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := kube.Create(context.Background(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "target"}}); err != nil {
		t.Fatal(err)
	}
	_, cfg, creds, now := fixture(t)
	snapshot, _ := cfg.Load()
	snapshot.Namespaces = config.NameSelector{Names: []string{"target"}}
	cfg.Apply(snapshot)
	if err := kube.Create(context.Background(), rendered(t, "target", cfg, creds)); err != nil {
		t.Fatal(err)
	}
	type replica struct {
		registry *prometheus.Registry
		cancel   context.CancelFunc
	}
	replicas := make([]replica, 2)
	for index := range replicas {
		cacheOptions, err := controller.NewCacheOptions(controller.ControllerOptions{})
		if err != nil {
			t.Fatal(err)
		}
		lease, renew, retry := 3*time.Second, 2*time.Second, 500*time.Millisecond
		manager, err := ctrl.NewManager(restConfig, ctrl.Options{Cache: cacheOptions, Logger: logr.Discard(), Metrics: metricsserver.Options{BindAddress: "0"}, HealthProbeBindAddress: "0", LeaderElection: true, LeaderElectionID: "metrics-handoff", LeaderElectionNamespace: "target", LeaderElectionReleaseOnCancel: true, LeaseDuration: &lease, RenewDeadline: &renew, RetryPeriod: &retry})
		if err != nil {
			t.Fatal(err)
		}
		for _, object := range []client.Object{&corev1.Namespace{}, &corev1.Secret{}} {
			if _, err := manager.GetCache().GetInformer(context.Background(), object, cache.BlockUntilSynced(false)); err != nil {
				t.Fatal(err)
			}
		}
		// Each replica has an independent credential store, initially empty.
		// Existing copies must remain observable before credential recovery.
		_, replicaConfig, replicaCredentials, _ := fixture(t)
		replicaConfig.Apply(snapshot)
		for key := range snapshot.Registries {
			replicaCredentials.Delete(key)
		}
		reg := prometheus.NewRegistry()
		m, err := New(reg, replicaConfig, replicaCredentials, Options{Reader: manager.GetCache(), WaitForCacheSync: manager.GetCache().WaitForCacheSync, SecretName: "auto-patch-secret", Now: func() time.Time { return now }})
		if err != nil {
			t.Fatal(err)
		}
		if err := manager.Add(m); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- manager.Start(ctx) }()
		t.Cleanup(func() { stopRunnable(t, cancel, done) })
		replicas[index] = replica{registry: reg, cancel: cancel}
	}
	leader := -1
	await(t, func() bool {
		a, _ := gauge(t, replicas[0].registry, "observation_success", nil)
		b, _ := gauge(t, replicas[1].registry, "observation_success", nil)
		if a+b != 1 {
			return false
		}
		if a == 1 {
			leader = 0
		} else {
			leader = 1
		}
		return true
	}, "exactly one observable leader")
	follower := 1 - leader
	if _, found := gauge(t, replicas[follower].registry, "secret_targets", nil); found {
		t.Fatal("follower exported business state")
	}
	if pending, _ := gauge(t, replicas[leader].registry, "secret_targets", map[string]string{"state": "pending"}); pending != 1 {
		t.Fatal("existing copy without in-memory credential must be pending")
	}
	replicas[leader].cancel()
	await(t, func() bool {
		old, _ := gauge(t, replicas[leader].registry, "active", nil)
		next, _ := gauge(t, replicas[follower].registry, "observation_success", nil)
		return old == 0 && next == 1
	}, "leadership handoff")
	if _, found := gauge(t, replicas[leader].registry, "secret_targets", nil); found {
		t.Fatal("old leader retained business gauges")
	}
	if expiry, found := gauge(t, replicas[follower].registry, "oldest_secret_expiration_timestamp_seconds", nil); !found || expiry != float64(now.Add(time.Hour).Unix()) {
		t.Fatal("new leader lost visibility of existing copy expiration")
	}
}

func await(t *testing.T, predicate func() bool, message string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if predicate() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", message)
}

func stopRunnable(t *testing.T, cancel context.CancelFunc, done <-chan error) {
	t.Helper()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Error(err)
		}
	case <-time.After(10 * time.Second):
		t.Error("Runnable did not stop")
	}
}
