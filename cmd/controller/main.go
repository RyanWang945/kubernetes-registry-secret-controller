package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/utils/clock"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/RyanWang945/kubernetes-registry-secret-controller/internal/config"
	"github.com/RyanWang945/kubernetes-registry-secret-controller/internal/controller"
	"github.com/RyanWang945/kubernetes-registry-secret-controller/internal/credential"
	"github.com/RyanWang945/kubernetes-registry-secret-controller/internal/registrysecret"
	serviceaccountsyncer "github.com/RyanWang945/kubernetes-registry-secret-controller/internal/serviceaccount"
)

const (
	userAgent               = "kubernetes-registry-secret-controller"
	defaultMetricsAddress   = ":8080"
	defaultHealthAddress    = ":8081"
	gracefulShutdownTimeout = 30 * time.Second
)

func main() {
	handler := slog.NewJSONHandler(os.Stdout, nil)
	slog.SetDefault(slog.New(handler))
	logger := logr.FromSlogHandler(handler)
	ctrl.SetLogger(logger)

	if err := run(logger); err != nil {
		logger.Error(err, "controller exited")
		os.Exit(1)
	}
}

func run(logger logr.Logger) error {
	var (
		kubeconfig     string
		metricsAddress string
		healthAddress  string
		leaderElection bool
	)
	flag.StringVar(&kubeconfig, "kubeconfig", "", "path to a kubeconfig for local development; in-cluster configuration is used by default")
	flag.StringVar(&metricsAddress, "metrics-bind-address", defaultMetricsAddress, "address for the Prometheus metrics endpoint; set to 0 to disable")
	flag.StringVar(&healthAddress, "health-probe-bind-address", defaultHealthAddress, "address for liveness and readiness probes; set to 0 to disable")
	flag.BoolVar(&leaderElection, "leader-elect", true, "enable leader election for the controller manager")
	flag.Parse()

	restConfig, err := loadRESTConfig(kubeconfig)
	if err != nil {
		return fmt.Errorf("load Kubernetes client configuration: %w", err)
	}
	restConfig.UserAgent = userAgent

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		return fmt.Errorf("register Kubernetes scheme: %w", err)
	}

	controllerOptions := controller.ControllerOptions{}
	cacheOptions, err := controller.NewCacheOptions(controllerOptions)
	if err != nil {
		return fmt.Errorf("configure controller cache: %w", err)
	}

	mgr, err := ctrl.NewManager(restConfig, ctrl.Options{
		Scheme:                        scheme,
		Cache:                         cacheOptions,
		Logger:                        logger,
		LeaderElection:                leaderElection,
		LeaderElectionID:              controller.DefaultLeaderElectionID,
		LeaderElectionNamespace:       controller.DefaultControllerNamespace,
		LeaderElectionReleaseOnCancel: true,
		Metrics:                       metricsserver.Options{BindAddress: metricsAddress},
		HealthProbeBindAddress:        healthAddress,
		GracefulShutdownTimeout:       ptr.To(gracefulShutdownTimeout),
	})
	if err != nil {
		return fmt.Errorf("create controller manager: %w", err)
	}

	configStore := &config.Store{}
	credentialStore := &credential.Store{}
	resourceEvents, err := controller.NewResourceEventPublisher(mgr.GetClient())
	if err != nil {
		return fmt.Errorf("create resource event publisher: %w", err)
	}
	credentialScheduler, err := credential.NewScheduler(
		configStore,
		credentialStore,
		credential.NewACRTokenProvider(),
		resourceEvents,
		clock.RealClock{},
		credential.SchedulerOptions{},
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
		"starting controller manager",
		"config_namespace", controller.DefaultControllerNamespace,
		"config_name", controller.DefaultConfigMapName,
		"managed_secret_name", controller.DefaultManagedSecretName,
		"max_concurrent_namespace_reconciles", controller.DefaultMaxConcurrentNamespaceReconciles,
		"max_concurrent_service_account_reconciles", controller.DefaultMaxConcurrentServiceAccountReconciles,
		"leader_election", leaderElection,
	)
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
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
