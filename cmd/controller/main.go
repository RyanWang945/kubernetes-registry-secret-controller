package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math"
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
	defaultKubeAPIQPS       = float64(rest.DefaultQPS)
	defaultKubeAPIBurst     = rest.DefaultBurst
)

type commandOptions struct {
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

	if err := run(logger, options); err != nil {
		logger.Error(err, "controller exited")
		os.Exit(1)
	}
}

func parseCommandOptions(args []string, output io.Writer) (commandOptions, error) {
	options := commandOptions{}
	flags := flag.NewFlagSet("controller", flag.ContinueOnError)
	flags.SetOutput(output)
	flags.StringVar(&options.kubeconfig, "kubeconfig", "", "path to a kubeconfig for local development; in-cluster configuration is used by default")
	flags.StringVar(&options.metricsAddress, "metrics-bind-address", defaultMetricsAddress, "address for the Prometheus metrics endpoint; set to 0 to disable")
	flags.StringVar(&options.healthAddress, "health-probe-bind-address", defaultHealthAddress, "address for liveness and readiness probes; set to 0 to disable")
	flags.BoolVar(&options.leaderElection, "leader-elect", true, "enable leader election for the controller manager")
	flags.Float64Var(&options.kubeAPIQPS, "kube-api-qps", defaultKubeAPIQPS, "maximum sustained Kubernetes API client requests per second")
	flags.IntVar(&options.kubeAPIBurst, "kube-api-burst", defaultKubeAPIBurst, "maximum Kubernetes API client burst above the sustained rate")
	flags.IntVar(
		&options.maxConcurrentNamespaceReconciles,
		"max-concurrent-namespace-reconciles",
		controller.DefaultMaxConcurrentNamespaceReconciles,
		"maximum concurrent namespace Secret reconciles",
	)
	flags.IntVar(
		&options.maxConcurrentServiceAccountReconciles,
		"max-concurrent-service-account-reconciles",
		controller.DefaultMaxConcurrentServiceAccountReconciles,
		"maximum concurrent ServiceAccount reconciles",
	)
	if err := flags.Parse(args); err != nil {
		return commandOptions{}, err
	}
	if flags.NArg() != 0 {
		return commandOptions{}, fmt.Errorf("unexpected positional arguments: %v", flags.Args())
	}
	if options.kubeAPIQPS <= 0 || options.kubeAPIQPS > math.MaxFloat32 || math.IsNaN(options.kubeAPIQPS) || math.IsInf(options.kubeAPIQPS, 0) {
		return commandOptions{}, fmt.Errorf("kube API QPS must be a finite positive value no greater than %g, got %v", math.MaxFloat32, options.kubeAPIQPS)
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

	controllerOptions := controller.ControllerOptions{
		MaxConcurrentNamespaceReconciles:      options.maxConcurrentNamespaceReconciles,
		MaxConcurrentServiceAccountReconciles: options.maxConcurrentServiceAccountReconciles,
	}
	cacheOptions, err := controller.NewCacheOptions(controllerOptions)
	if err != nil {
		return fmt.Errorf("configure controller cache: %w", err)
	}

	mgr, err := ctrl.NewManager(restConfig, ctrl.Options{
		Scheme:                        scheme,
		Cache:                         cacheOptions,
		Logger:                        logger,
		LeaderElection:                options.leaderElection,
		LeaderElectionID:              controller.DefaultLeaderElectionID,
		LeaderElectionNamespace:       controller.DefaultControllerNamespace,
		LeaderElectionReleaseOnCancel: true,
		Metrics:                       metricsserver.Options{BindAddress: options.metricsAddress},
		HealthProbeBindAddress:        options.healthAddress,
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
		"kube_api_qps", options.kubeAPIQPS,
		"kube_api_burst", options.kubeAPIBurst,
		"max_concurrent_namespace_reconciles", options.maxConcurrentNamespaceReconciles,
		"max_concurrent_service_account_reconciles", options.maxConcurrentServiceAccountReconciles,
		"leader_election", options.leaderElection,
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
