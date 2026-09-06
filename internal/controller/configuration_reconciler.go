package controller

import (
	"context"
	"errors"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/RyanWang945/kubernetes-registry-secret-controller/internal/config"
)

// ConfigurationReconciler owns loading and validating the single controller
// ConfigMap. It runs on every replica so each process has an independently
// validated, last-known-good configuration snapshot.
type ConfigurationReconciler struct {
	client        client.Client
	store         *config.Store
	observer      ConfigurationObserver
	publisher     *ResourceEventPublisher
	options       ControllerOptions
	recorder      record.EventRecorder
	lastRejection string
	pendingFanout bool
}

// RuntimeConfigurationUpdater applies validated tuning on every replica.
// Implementations return quickly and must not perform network requests.
type RuntimeConfigurationUpdater interface {
	Apply(context.Context, config.RuntimeConfiguration) error
}

func (r *ConfigurationReconciler) Reconcile(ctx context.Context, request ctrl.Request) (ctrl.Result, error) {
	if request.Namespace != r.options.ControllerNamespace || request.Name != r.options.ConfigMapName {
		return ctrl.Result{}, nil
	}

	logger := log.FromContext(ctx)
	configMap := &corev1.ConfigMap{}
	if err := r.client.Get(ctx, request.NamespacedName, configMap); err != nil {
		if ctx.Err() != nil && errors.Is(err, context.Canceled) {
			return ctrl.Result{}, nil
		}
		if apierrors.IsNotFound(err) {
			r.store.MarkInvalid()
			if r.lastRejection != "missing" {
				logger.Error(errors.New("configuration is missing"), "retaining the last valid configuration", "operation", "load_configuration", "reason", "ConfigMapMissing")
				r.lastRejection = "missing"
			}
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("get fixed ConfigMap: %w", err)
	}

	candidate, err := config.Parse(configMap.Data)
	if err == nil && r.options.RuntimeUpdater != nil {
		err = r.options.RuntimeUpdater.Apply(ctx, candidate.Runtime)
	}
	if err != nil {
		r.store.MarkInvalid()
		rejection := string(configMap.UID) + "/" + configMap.ResourceVersion
		if r.lastRejection != rejection {
			// Parser errors can contain user-provided field names or values.
			logger.Error(errors.New("configuration validation failed; check selectors, registry fields and runtime tuning"), "retaining the last valid configuration", "operation", "validate_configuration", "reason", "InvalidConfiguration")
			if r.recorder != nil {
				r.recorder.Event(configMap, corev1.EventTypeWarning, "InvalidConfiguration", "Configuration validation failed; check selectors, registry fields and runtime tuning. Retaining the last valid configuration.")
			}
			r.lastRejection = rejection
		}
		return ctrl.Result{}, nil
	}
	r.lastRejection = ""

	result := r.store.Apply(candidate)
	if result.BusinessChanged {
		r.pendingFanout = true
		r.observer.NotifyConfigurationChanged()
	}
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

	// Keep unfinished business fan-out pending across retries, including tuning
	// updates that arrive after Store.Apply. Completed tuning-only or equivalent
	// updates do not enqueue every Namespace/ServiceAccount again.
	if !r.pendingFanout {
		return ctrl.Result{}, nil
	}
	if err := r.publisher.PublishAllNamespaces(ctx); err != nil {
		if ctx.Err() != nil && errors.Is(err, context.Canceled) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("publish configuration Namespace events: %w", err)
	}
	if err := r.publisher.PublishAllServiceAccounts(ctx); err != nil {
		if ctx.Err() != nil && errors.Is(err, context.Canceled) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("publish configuration ServiceAccount events: %w", err)
	}
	r.pendingFanout = false
	return ctrl.Result{}, nil
}
