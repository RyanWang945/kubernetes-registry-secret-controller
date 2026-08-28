package controller

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/RyanWang945/kubernetes-registry-secret-controller/internal/config"
)

// ConfigurationReconciler owns loading and validating the single controller
// ConfigMap. It runs on every replica so each process has an independently
// validated, last-known-good configuration snapshot.
type ConfigurationReconciler struct {
	client          client.Client
	store           *config.Store
	namespaceEvents chan<- event.GenericEvent
	options         ControllerOptions
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
	if err := r.publishAllNamespaces(ctx); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

func (r *ConfigurationReconciler) publishAllNamespaces(ctx context.Context) error {
	namespaces := &corev1.NamespaceList{}
	if err := r.client.List(ctx, namespaces); err != nil {
		return fmt.Errorf("list Namespaces for configuration fan-out: %w", err)
	}

	for i := range namespaces.Items {
		namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespaces.Items[i].Name}}
		select {
		case r.namespaceEvents <- event.GenericEvent{Object: namespace}:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}
