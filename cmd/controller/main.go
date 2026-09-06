package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
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
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	"github.com/RyanWang945/kubernetes-registry-secret-controller/internal/config"
	"github.com/RyanWang945/kubernetes-registry-secret-controller/internal/controller"
	"github.com/RyanWang945/kubernetes-registry-secret-controller/internal/credential"
	"github.com/RyanWang945/kubernetes-registry-secret-controller/internal/kubeclient"
	"github.com/RyanWang945/kubernetes-registry-secret-controller/internal/observability"
	"github.com/RyanWang945/kubernetes-registry-secret-controller/internal/registrysecret"
	serviceaccountsyncer "github.com/RyanWang945/kubernetes-registry-secret-controller/internal/serviceaccount"
)

const (
	userAgent               = "kubernetes-registry-secret-controller"
	defaultMetricsAddress   = ":8080"
	defaultHealthAddress    = ":8081"
	gracefulShutdownTimeout = 30 * time.Second
	defaultKubeAPIQPS       = config.DefaultKubeAPIQPS
	defaultKubeAPIBurst     = config.DefaultKubeAPIBurst
)

type commandOptions struct {
	metricsSecure                         bool
	metricsCertDir                        string
	logLevel                              string
	kubeconfig                            string
	metricsAddress                        string
	healthAddress                         string
	leaderElection                        bool
	kubeAPIQPS                            float64
	kubeAPIBurst                          int
	maxConcurrentNamespaceReconciles      int
	maxConcurrentServiceAccountReconciles int
}

func main() {
	handler := slog.NewJSONHandler(os.Stdout, nil)
	slog.SetDefault(slog.New(handler))
	logger := logr.FromSlogHandler(handler)
	ctrl.SetLogger(logger)

	options, err := parseCommandOptions(os.Args[1:], os.Stderr)
	if errors.Is(err, flag.ErrHelp) {
		return
	}
	if err != nil {
		logger.Error(err, "invalid command-line options")
		os.Exit(2)
	}

	logger, _ = observability.ConfigureLogging(os.Stdout, options.logLevel)
	ctrl.SetLogger(logger)

	if err := run(logger, options); err != nil {
		logger.Error(err, "controller exited")
		os.Exit(1)
	}
}

func parseCommandOptions(args []string, output io.Writer) (commandOptions, error) {
	options := commandOptions{}
	flags := flag.NewFlagSet("controller", flag.ContinueOnError)
	flags.SetOutput(output)
	flags.BoolVar(&options.metricsSecure, "metrics-secure", false, "serve metrics with TLS and Kubernetes authentication/authorization")
	flags.StringVar(&options.metricsCertDir, "metrics-cert-dir", "", "directory containing metrics tls.crt and tls.key")
	flags.StringVar(&options.logLevel, "log-level", "info", "logging verbosity: debug, info, warn or error")
	flags.StringVar(&options.kubeconfig, "kubeconfig", "", "path to a kubeconfig for local development; in-cluster configuration is used by default")
	flags.StringVar(&options.metricsAddress, "metrics-bind-address", defaultMetricsAddress, "address for the Prometheus metrics endpoint; set to 0 to disable")
	flags.StringVar(&options.healthAddress, "health-probe-bind-address", defaultHealthAddress, "address for liveness and readiness probes; set to 0 to disable")
	flags.BoolVar(&options.leaderElection, "leader-elect", true, "enable leader election for the controller manager")
	flags.Float64Var(&options.kubeAPIQPS, "kube-api-qps", defaultKubeAPIQPS, "startup/fallback Kubernetes API QPS; ConfigMap overrides the business client live")
	flags.IntVar(&options.kubeAPIBurst, "kube-api-burst", defaultKubeAPIBurst, "startup/fallback Kubernetes API burst; ConfigMap overrides the business client live")
	flags.IntVar(
		&options.maxConcurrentNamespaceReconciles,
		"max-concurrent-namespace-reconciles",
		controller.DefaultMaxConcurrentNamespaceReconciles,
		"startup/fallback namespace Secret workers; ConfigMap workers overrides on restart",
	)
	flags.IntVar(
		&options.maxConcurrentServiceAccountReconciles,
		"max-concurrent-service-account-reconciles",
		controller.DefaultMaxConcurrentServiceAccountReconciles,
		"startup/fallback ServiceAccount workers; ConfigMap workers overrides on restart",
	)
	if err := flags.Parse(args); err != nil {
		return commandOptions{}, err
	}
	if _, err := observability.LogLevel(options.logLevel); err != nil {
		return commandOptions{}, err
	}
	if flags.NArg() != 0 {
		return commandOptions{}, fmt.Errorf("unexpected positional arguments: %v", flags.Args())
	}
	if err := config.ValidateQPS(options.kubeAPIQPS); err != nil {
		return commandOptions{}, err
	}
	if options.kubeAPIBurst < 1 {
		return commandOptions{}, fmt.Errorf("kube API burst must be at least one, got %d", options.kubeAPIBurst)
	}
	if options.maxConcurrentNamespaceReconciles < 1 {
		return commandOptions{}, fmt.Errorf(
			"maximum concurrent namespace reconciles must be at least one, got %d",
			options.maxConcurrentNamespaceReconciles,
		)
	}
	if options.maxConcurrentServiceAccountReconciles < 1 {
		return commandOptions{}, fmt.Errorf(
			"maximum concurrent ServiceAccount reconciles must be at least one, got %d",
			options.maxConcurrentServiceAccountReconciles,
		)
	}
	return options, nil
}

func run(logger logr.Logger, options commandOptions) error {
	ctx := log.IntoContext(ctrl.SetupSignalHandler(), logger)
	restConfig, err := loadRESTConfig(options.kubeconfig)
	if err != nil {
		return fmt.Errorf("load Kubernetes client configuration: %w", err)
	}
	restConfig.UserAgent = userAgent
	restConfig.QPS = float32(options.kubeAPIQPS)
	restConfig.Burst = options.kubeAPIBurst

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		return fmt.Errorf("register Kubernetes scheme: %w", err)
	}
	runtimeSettings, err := kubeclient.Bootstrap(ctx, restConfig, client.ObjectKey{Namespace: controller.DefaultControllerNamespace, Name: controller.DefaultConfigMapName}, kubeclient.Settings{
		QPS: options.kubeAPIQPS, Burst: options.kubeAPIBurst,
		NamespaceWorkers: options.maxConcurrentNamespaceReconciles, ServiceAccountWorkers: options.maxConcurrentServiceAccountReconciles,
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

	metricsOptions, err := observability.ServerOptions(options.metricsAddress, options.metricsSecure, options.metricsCertDir)
	if err != nil {
		return err
	}
	mgr, err := ctrl.NewManager(restConfig, ctrl.Options{
		NewClient:                     runtimeSettings.NewClient,
		Scheme:                        scheme,
		Cache:                         cacheOptions,
		Logger:                        logger,
		LeaderElection:                options.leaderElection,
		LeaderElectionID:              controller.DefaultLeaderElectionID,
		LeaderElectionNamespace:       controller.DefaultControllerNamespace,
		LeaderElectionReleaseOnCancel: true,
		Metrics:                       metricsOptions,
		HealthProbeBindAddress:        options.healthAddress,
		GracefulShutdownTimeout:       ptr.To(gracefulShutdownTimeout),
	})
	if err != nil {
		return fmt.Errorf("create controller manager: %w", err)
	}

	configStore := &config.Store{}
	credentialStore := &credential.Store{}
	obs, err := observability.New(ctrlmetrics.Registry, configStore, credentialStore, observability.Options{
		Reader: mgr.GetCache(), WaitForCacheSync: mgr.GetCache().WaitForCacheSync, SecretName: controller.DefaultManagedSecretName,
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
		obs.WrapProvider(credential.NewACRTokenProvider()),
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
		"starting controller manager",
		"config_namespace", controller.DefaultControllerNamespace,
		"config_name", controller.DefaultConfigMapName,
		"managed_secret_name", controller.DefaultManagedSecretName,
		"kube_api_qps", initial.QPS,
		"kube_api_burst", initial.Burst,
		"max_concurrent_namespace_reconciles", initial.NamespaceWorkers,
		"max_concurrent_service_account_reconciles", initial.ServiceAccountWorkers,
		"leader_election", options.leaderElection,
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
