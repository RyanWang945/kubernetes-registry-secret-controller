package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/RyanWang945/kubernetes-registry-secret-controller/internal/config"
	"github.com/RyanWang945/kubernetes-registry-secret-controller/internal/controller"
)

const userAgent = "kubernetes-registry-secret-controller"

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	if err := run(logger); err != nil {
		logger.Error("controller exited", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	var kubeconfig string
	flag.StringVar(&kubeconfig, "kubeconfig", "", "path to a kubeconfig for local development; in-cluster configuration is used by default")
	flag.Parse()

	restConfig, err := loadRESTConfig(kubeconfig)
	if err != nil {
		return fmt.Errorf("load Kubernetes client configuration: %w", err)
	}
	restConfig.UserAgent = userAgent

	client, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return fmt.Errorf("create Kubernetes client: %w", err)
	}

	configStore := &config.Store{}
	syncer := controller.NewLoggingSyncer(logger)
	resourceController, err := controller.New(client, configStore, syncer, controller.ControllerOptions{Logger: logger})
	if err != nil {
		return fmt.Errorf("create controller: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	logger.Info(
		"starting controller listener skeleton",
		"config_namespace", controller.DefaultControllerNamespace,
		"config_name", controller.DefaultConfigMapName,
		"resource_workers", controller.DefaultResourceWorkers,
	)
	if err := resourceController.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		return err
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
