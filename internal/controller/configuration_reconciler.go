package controller

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/RyanWang945/kubernetes-registry-secret-controller/internal/config"
)

// ConfigurationReconciler owns loading and validating the single controller
// ConfigMap. It runs on every replica so each process has an independently
// validated, last-known-good configuration snapshot.
type ConfigurationReconciler struct {
	client    client.Client
	store     *config.Store
	observer  ConfigurationObserver
	publisher *ResourceEventPublisher
	options   ControllerOptions
}

func (r *ConfigurationReconciler) Reconcile(ctx context.Context, request ctrl.Request) (ctrl.Result, error) {
	if request.Namespace != r.options.ControllerNamespace || request.Name != r.options.ConfigMapName {
		return ctrl.Result{}, nil
	}

	logger := log.FromContext(ctx)
	configMap := &corev1.ConfigMap{}
	if err := r.client.Get(ctx, request.NamespacedName, configMap); err != nil {
		if apierrors.IsNotFound(err) {
			logger.Error(err, "fixed ConfigMap is unavailable; retaining the last valid configuration")
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("get fixed ConfigMap: %w", err)
	}

	candidate, err := config.Parse(configMap.Data)
	if err != nil {
		logger.Error(err, "rejected invalid ConfigMap; retaining the last valid configuration")
		return ctrl.Result{}, nil
	}

	result := r.store.Apply(candidate)
	r.observer.NotifyConfigurationChanged()
	if result.Changed {
		logger.Info(
			"applied valid ConfigMap",
			"generation", result.Current.Generation,
			"registries", len(result.Current.Registries),
		)
	} else {
		logger.V(1).Info(
			"ConfigMap has no semantic changes",
			"generation", result.Current.Generation,
		)
	}

	// Fan out on every successful reconciliation, not only when Store reports a
	// semantic change. If listing or publishing fails after Store.Apply, the
	// controller retry can therefore finish the fan-out idempotently.
	if err := r.publisher.PublishAllNamespaces(ctx); err != nil {
		return ctrl.Result{}, fmt.Errorf("publish configuration Namespace events: %w", err)
	}
	if err := r.publisher.PublishAllServiceAccounts(ctx); err != nil {
		return ctrl.Result{}, fmt.Errorf("publish configuration ServiceAccount events: %w", err)
	}
	return ctrl.Result{}, nil
}
