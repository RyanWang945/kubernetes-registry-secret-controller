// Command e2e-controller runs the real controller wiring with a deliberately
// fake credential provider. It exists only for end-to-end tests and refuses to
// start unless --allow-mock-provider is set explicitly.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"sync/atomic"
	"time"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/utils/clock"
	clocktesting "k8s.io/utils/clock/testing"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/RyanWang945/kubernetes-registry-secret-controller/internal/config"
	"github.com/RyanWang945/kubernetes-registry-secret-controller/internal/controller"
	"github.com/RyanWang945/kubernetes-registry-secret-controller/internal/credential"
	"github.com/RyanWang945/kubernetes-registry-secret-controller/internal/kubeclient"
	"github.com/RyanWang945/kubernetes-registry-secret-controller/internal/observability"
	"github.com/RyanWang945/kubernetes-registry-secret-controller/internal/registrysecret"
	serviceaccountsyncer "github.com/RyanWang945/kubernetes-registry-secret-controller/internal/serviceaccount"
)

const (
	e2eUserAgent            = "kubernetes-registry-secret-controller-e2e"
	e2eLeaderElectionID     = "kubernetes-registry-secret-controller-e2e"
	defaultMetricsAddress   = ":8080"
	defaultHealthAddress    = ":8081"
	defaultMockTokenTTL     = 30 * time.Second
	defaultRefreshBefore    = 10 * time.Second
	defaultRequestTimeout   = 5 * time.Second
	defaultRetryBaseDelay   = time.Second
	defaultRetryMaxDelay    = 5 * time.Second
	gracefulShutdownTimeout = 10 * time.Second
	defaultKubeAPIQPS       = config.DefaultKubeAPIQPS
	defaultKubeAPIBurst     = config.DefaultKubeAPIBurst
)

type mockTokenProvider struct {
	providerID        string
	tokenTTL          time.Duration
	clock             clock.PassiveClock
	calls             atomic.Uint64
	remainingFailures atomic.Uint64
}

func newMockTokenProvider(
	providerID string,
	tokenTTL time.Duration,
	failures uint64,
	providerClock clock.PassiveClock,
) *mockTokenProvider {
	provider := &mockTokenProvider{
		providerID: providerID,
		tokenTTL:   tokenTTL,
		clock:      providerClock,
	}
	provider.remainingFailures.Store(failures)
	return provider
}

func (p *mockTokenProvider) GetAuthorizationToken(ctx context.Context, request credential.Request) (credential.Token, error) {
	select {
	case <-ctx.Done():
		return credential.Token{}, ctx.Err()
	default:
	}

	call := p.calls.Add(1)
	for {
		remaining := p.remainingFailures.Load()
		if remaining == 0 {
			break
		}
		if p.remainingFailures.CompareAndSwap(remaining, remaining-1) {
			slog.Warn(
				"mock token request failed intentionally",
				"provider_id", p.providerID,
				"registry", request.Key.String(),
				"call", call,
			)
			return credential.Token{}, errors.New("intentional mock provider failure")
		}
	}

	expiresAt := p.clock.Now().Add(p.tokenTTL).UTC()
	slog.Info(
		"mock token issued",
		"provider_id", p.providerID,
		"registry", request.Key.String(),
		"call", call,
		"expires_at", expiresAt,
	)
	return credential.Token{
		Username:  "mock-user",
		Password:  fmt.Sprintf("mock-token-%s-%d", p.providerID, call),
		ExpiresAt: expiresAt,
	}, nil
}

func main() {
	handler := slog.NewJSONHandler(os.Stdout, nil)
	slog.SetDefault(slog.New(handler))
	logger := logr.FromSlogHandler(handler)
	ctrl.SetLogger(logger)

	if err := run(logger); err != nil {
		logger.Error(err, "E2E controller exited")
		os.Exit(1)
	}
}

func run(logger logr.Logger) error {
	var (
		logLevel                  string
		kubeconfig                string
		metricsAddress            string
		healthAddress             string
		leaderElection            bool
		allowMock                 bool
		mockTokenTTL              time.Duration
		refreshBefore             time.Duration
		mockFailures              uint64
		clockControl              string
		kubeAPIQPS                float64
		kubeAPIBurst              int
		namespaceConcurrency      int
		serviceAccountConcurrency int
	)
	flag.StringVar(&logLevel, "log-level", "info", "logging verbosity: debug, info, warn or error")
	flag.StringVar(&metricsAddress, "metrics-bind-address", defaultMetricsAddress, "address for the Prometheus metrics endpoint")
	flag.StringVar(&healthAddress, "health-probe-bind-address", defaultHealthAddress, "address for liveness and readiness probes")
	flag.BoolVar(&leaderElection, "leader-elect", true, "enable leader election")
	flag.BoolVar(&allowMock, "allow-mock-provider", false, "required safety acknowledgement for the fake credential provider")
	flag.DurationVar(&mockTokenTTL, "mock-token-ttl", defaultMockTokenTTL, "validity of credentials returned by the mock provider")
	flag.DurationVar(&refreshBefore, "credential-refresh-before", defaultRefreshBefore, "how long before expiry to refresh the mock credential")
	flag.Uint64Var(&mockFailures, "mock-initial-failures", 0, "number of initial provider requests to fail in this process")
	flag.StringVar(&clockControl, "mock-clock-control-address", "", "test-only HTTP address for advancing a fake scheduler clock; empty uses real time")
	flag.Float64Var(&kubeAPIQPS, "kube-api-qps", defaultKubeAPIQPS, "startup/fallback Kubernetes API QPS; ConfigMap overrides the business client live")
	flag.IntVar(&kubeAPIBurst, "kube-api-burst", defaultKubeAPIBurst, "startup/fallback Kubernetes API burst; ConfigMap overrides the business client live")
	flag.IntVar(
		&namespaceConcurrency,
		"max-concurrent-namespace-reconciles",
		controller.DefaultMaxConcurrentNamespaceReconciles,
		"startup/fallback namespace Secret workers; ConfigMap workers overrides on restart",
	)
	flag.IntVar(
		&serviceAccountConcurrency,
		"max-concurrent-service-account-reconciles",
		controller.DefaultMaxConcurrentServiceAccountReconciles,
		"startup/fallback ServiceAccount workers; ConfigMap workers overrides on restart",
	)
	flag.Parse()
	var logErr error
	logger, logErr = observability.ConfigureLogging(os.Stdout, logLevel)
	if logErr != nil {
		return logErr
	}
	ctrl.SetLogger(logger)
	if kubeconfigFlag := flag.Lookup("kubeconfig"); kubeconfigFlag != nil {
		kubeconfig = kubeconfigFlag.Value.String()
	}
	ctx := log.IntoContext(ctrl.SetupSignalHandler(), logger)

	if !allowMock {
		return errors.New("refusing to start the E2E controller without --allow-mock-provider")
	}
	if mockTokenTTL <= refreshBefore {
		return fmt.Errorf("mock token TTL %s must be greater than refresh-before %s", mockTokenTTL, refreshBefore)
	}
	if err := config.ValidateQPS(kubeAPIQPS); err != nil {
		return err
	}
	if kubeAPIBurst < 1 {
		return fmt.Errorf("kube API burst must be at least one, got %d", kubeAPIBurst)
	}
	if namespaceConcurrency < 1 {
		return fmt.Errorf("maximum concurrent namespace reconciles must be at least one, got %d", namespaceConcurrency)
	}
	if serviceAccountConcurrency < 1 {
		return fmt.Errorf("maximum concurrent ServiceAccount reconciles must be at least one, got %d", serviceAccountConcurrency)
	}

	restConfig, err := loadRESTConfig(kubeconfig)
	if err != nil {
		return fmt.Errorf("load Kubernetes client configuration: %w", err)
	}
	restConfig.UserAgent = e2eUserAgent
	restConfig.QPS = float32(kubeAPIQPS)
	restConfig.Burst = kubeAPIBurst

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		return fmt.Errorf("register Kubernetes scheme: %w", err)
	}
	runtimeSettings, err := kubeclient.Bootstrap(ctx, restConfig, client.ObjectKey{Namespace: controller.DefaultControllerNamespace, Name: controller.DefaultConfigMapName}, kubeclient.Settings{
		QPS: kubeAPIQPS, Burst: kubeAPIBurst,
		NamespaceWorkers: namespaceConcurrency, ServiceAccountWorkers: serviceAccountConcurrency,
	})
	if err != nil {
		if ctx.Err() != nil && errors.Is(err, context.Canceled) {
			return nil
		}
		return err
	}
	initial := runtimeSettings.Current()

	controllerOptions := controller.ControllerOptions{
		RuntimeUpdater:                        runtimeSettings,
		MaxConcurrentNamespaceReconciles:      initial.NamespaceWorkers,
		MaxConcurrentServiceAccountReconciles: initial.ServiceAccountWorkers,
	}
	cacheOptions, err := controller.NewCacheOptions(controllerOptions)
	if err != nil {
		return fmt.Errorf("configure controller cache: %w", err)
	}

	mgr, err := ctrl.NewManager(restConfig, ctrl.Options{
		NewClient:                     runtimeSettings.NewClient,
		Scheme:                        scheme,
		Cache:                         cacheOptions,
		Logger:                        logger,
		LeaderElection:                leaderElection,
		LeaderElectionID:              e2eLeaderElectionID,
		LeaderElectionNamespace:       controller.DefaultControllerNamespace,
		LeaderElectionReleaseOnCancel: true,
		Metrics:                       metricsserver.Options{BindAddress: metricsAddress},
		HealthProbeBindAddress:        healthAddress,
		GracefulShutdownTimeout:       ptr.To(gracefulShutdownTimeout),
	})
	if err != nil {
		return fmt.Errorf("create controller manager: %w", err)
	}

	var schedulerClock clock.WithTicker = clock.RealClock{}
	if clockControl != "" {
		fakeClock := clocktesting.NewFakeClock(time.Now().UTC())
		schedulerClock = fakeClock
		if err := mgr.Add(newMockClockServer(clockControl, fakeClock)); err != nil {
			return fmt.Errorf("register mock clock control server: %w", err)
		}
	}

	providerID, err := os.Hostname()
	if err != nil || providerID == "" {
		providerID = "unknown"
	}
	configStore := &config.Store{}
	credentialStore := &credential.Store{}
	obs, err := observability.New(ctrlmetrics.Registry, configStore, credentialStore, observability.Options{
		Reader: mgr.GetCache(), WaitForCacheSync: mgr.GetCache().WaitForCacheSync, SecretName: controller.DefaultManagedSecretName, Now: schedulerClock.Now,
	})
	if err != nil {
		return fmt.Errorf("create observability: %w", err)
	}
	if err = mgr.Add(obs); err != nil {
		return fmt.Errorf("register observability: %w", err)
	}
	resourceEvents, err := controller.NewResourceEventPublisher(mgr.GetClient())
	if err != nil {
		return fmt.Errorf("create resource event publisher: %w", err)
	}
	credentialScheduler, err := credential.NewScheduler(
		configStore,
		credentialStore,
		obs.WrapProvider(newMockTokenProvider(providerID, mockTokenTTL, mockFailures, schedulerClock)),
		resourceEvents,
		schedulerClock,
		credential.SchedulerOptions{
			RefreshBefore:  refreshBefore,
			RequestTimeout: defaultRequestTimeout,
			WorkerCount:    credential.DefaultWorkerCount,
			RetryBaseDelay: defaultRetryBaseDelay,
			RetryMaxDelay:  defaultRetryMaxDelay,
		},
	)
	if err != nil {
		return fmt.Errorf("create credential scheduler: %w", err)
	}
	if err := mgr.Add(credentialScheduler); err != nil {
		return fmt.Errorf("register credential scheduler: %w", err)
	}

	namespaceSecretSyncer, err := registrysecret.NewSyncer(
		mgr.GetClient(),
		configStore,
		credentialStore,
		controller.DefaultManagedSecretName,
		mgr.GetEventRecorderFor("namespace-secrets"),
	)
	if err != nil {
		return fmt.Errorf("create namespace Secret syncer: %w", err)
	}
	serviceAccountSyncer, err := serviceaccountsyncer.NewSyncer(
		mgr.GetClient(),
		configStore,
		controller.DefaultManagedSecretName,
	)
	if err != nil {
		return fmt.Errorf("create ServiceAccount syncer: %w", err)
	}
	if err := controller.SetupWithManager(
		mgr,
		configStore,
		credentialScheduler,
		resourceEvents,
		namespaceSecretSyncer,
		serviceAccountSyncer,
		controllerOptions,
	); err != nil {
		return fmt.Errorf("register controllers: %w", err)
	}

	if err := mgr.AddHealthzCheck("ping", healthz.Ping); err != nil {
		return fmt.Errorf("register liveness check: %w", err)
	}
	if err := mgr.AddReadyzCheck("configuration", controller.ConfigurationReadyCheck(configStore)); err != nil {
		return fmt.Errorf("register configuration readiness check: %w", err)
	}

	logger.Info(
		"starting E2E controller manager with mock provider",
		"provider_id", providerID,
		"mock_token_ttl", mockTokenTTL,
		"refresh_before", refreshBefore,
		"leader_election", leaderElection,
		"mock_clock_control_address", clockControl,
		"kube_api_qps", initial.QPS,
		"kube_api_burst", initial.Burst,
		"max_concurrent_namespace_reconciles", initial.NamespaceWorkers,
		"max_concurrent_service_account_reconciles", initial.ServiceAccountWorkers,
	)
	if err := mgr.Start(ctx); err != nil {
		return fmt.Errorf("run controller manager: %w", err)
	}
	return nil
}

func loadRESTConfig(kubeconfig string) (*rest.Config, error) {
	if kubeconfig != "" {
		return clientcmd.BuildConfigFromFlags("", kubeconfig)
	}
	if inCluster, err := rest.InClusterConfig(); err == nil {
		return inCluster, nil
	}
	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		loadingRules,
		&clientcmd.ConfigOverrides{},
	).ClientConfig()
}
