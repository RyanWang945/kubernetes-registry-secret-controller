package controller

import (
	"context"
	"errors"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	controllerRuntime "sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	"github.com/RyanWang945/kubernetes-registry-secret-controller/internal/config"
)

// SetupWithManager registers the always-on configuration controller and the
// leader-only namespace controller with one shared Manager.
func SetupWithManager(
	mgr manager.Manager,
	store *config.Store,
	syncer NamespaceSyncer,
	options ControllerOptions,
) error {
	if mgr == nil {
		return errors.New("manager must not be nil")
	}
	if store == nil {
		return errors.New("config store must not be nil")
	}
	if syncer == nil {
		return errors.New("namespace syncer must not be nil")
	}

	options = options.withDefaults()
	if err := options.validate(); err != nil {
		return err
	}

	namespaceEvents := make(chan event.GenericEvent, namespaceEventBuffer)
	namespaceReconciler := &NamespaceReconciler{store: store, syncer: syncer}

	if err := ctrl.NewControllerManagedBy(mgr).
		Named("namespace_resources").
		For(&corev1.Namespace{}).
		Watches(
			&corev1.ServiceAccount{},
			handler.EnqueueRequestsFromMapFunc(mapTargetServiceAccount(store)),
		).
		Watches(
			&corev1.Secret{},
			handler.EnqueueRequestsFromMapFunc(mapManagedSecret(options.ManagedSecretName)),
		).
		WatchesRawSource(source.Channel(namespaceEvents, &handler.EnqueueRequestForObject{})).
		WithOptions(controllerRuntime.Options{
			MaxConcurrentReconciles: options.MaxConcurrentNamespaceReconciles,
			NeedLeaderElection:      ptr.To(true),
			EnableWarmup:            ptr.To(true),
		}).
		Complete(namespaceReconciler); err != nil {
		return fmt.Errorf("register namespace controller: %w", err)
	}

	configurationReconciler := &ConfigurationReconciler{
		client:          mgr.GetClient(),
		store:           store,
		namespaceEvents: namespaceEvents,
		options:         options,
	}
	configMapPredicate := predicate.NewPredicateFuncs(func(object client.Object) bool {
		return object.GetNamespace() == options.ControllerNamespace && object.GetName() == options.ConfigMapName
	})

	if err := ctrl.NewControllerManagedBy(mgr).
		Named("configuration").
		For(&corev1.ConfigMap{}, builder.WithPredicates(configMapPredicate)).
		WithOptions(controllerRuntime.Options{NeedLeaderElection: ptr.To(false)}).
		Complete(configurationReconciler); err != nil {
		return fmt.Errorf("register configuration controller: %w", err)
	}

	return nil
}

func mapTargetServiceAccount(store *config.Store) handler.MapFunc {
	return func(_ context.Context, object client.Object) []reconcile.Request {
		serviceAccount, ok := object.(*corev1.ServiceAccount)
		if !ok || !store.MatchesServiceAccount(serviceAccount.Namespace, serviceAccount.Name) {
			return nil
		}
		return requestForNamespace(serviceAccount.Namespace)
	}
}

func mapManagedSecret(name string) handler.MapFunc {
	return func(_ context.Context, object client.Object) []reconcile.Request {
		secret, ok := object.(*corev1.Secret)
		if !ok || secret.Name != name {
			return nil
		}
		return requestForNamespace(secret.Namespace)
	}
}

func requestForNamespace(namespace string) []reconcile.Request {
	if namespace == "" {
		return nil
	}
	return []reconcile.Request{{NamespacedName: types.NamespacedName{Name: namespace}}}
}
