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
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	"github.com/RyanWang945/kubernetes-registry-secret-controller/internal/config"
	"github.com/RyanWang945/kubernetes-registry-secret-controller/internal/registrysecret"
)

// SetupWithManager registers the always-on configuration controller and the
// leader-only Namespace Secret and ServiceAccount controllers with one Manager.
func SetupWithManager(
	mgr manager.Manager,
	store *config.Store,
	configurationObserver ConfigurationObserver,
	resourceEvents *ResourceEventPublisher,
	namespaceSecretSyncer NamespaceSecretSyncer,
	serviceAccountSyncer ServiceAccountSyncer,
	options ControllerOptions,
) error {
	if mgr == nil {
		return errors.New("manager must not be nil")
	}
	if store == nil {
		return errors.New("config store must not be nil")
	}
	if configurationObserver == nil {
		return errors.New("configuration observer must not be nil")
	}
	if resourceEvents == nil {
		return errors.New("resource event publisher must not be nil")
	}
	if namespaceSecretSyncer == nil {
		return errors.New("namespace Secret syncer must not be nil")
	}
	if serviceAccountSyncer == nil {
		return errors.New("ServiceAccount syncer must not be nil")
	}

	options = options.withDefaults()
	if err := options.validate(); err != nil {
		return err
	}

	namespaceReconciler := &NamespaceSecretReconciler{store: store, syncer: namespaceSecretSyncer}

	if err := ctrl.NewControllerManagedBy(mgr).
		Named("namespace_secrets").
		Watches(&corev1.Namespace{}, &namespaceEventHandler{}).
		Watches(
			&corev1.Secret{},
			handler.EnqueueRequestsFromMapFunc(mapManagedSecretToNamespace(options.ManagedSecretName)),
		).
		WatchesRawSource(source.Channel(resourceEvents.namespaceEvents, &handler.EnqueueRequestForObject{})).
		WithOptions(controllerRuntime.Options{
			MaxConcurrentReconciles: options.MaxConcurrentNamespaceReconciles,
			NeedLeaderElection:      ptr.To(true),
			EnableWarmup:            ptr.To(true),
			UsePriorityQueue:        ptr.To(true),
		}).
		Complete(namespaceReconciler); err != nil {
		return fmt.Errorf("register namespace Secret controller: %w", err)
	}

	serviceAccountReconciler := newServiceAccountReconciler(store, serviceAccountSyncer)
	if err := ctrl.NewControllerManagedBy(mgr).
		Named("service_accounts").
		Watches(&corev1.ServiceAccount{}, &serviceAccountEventHandler{}).
		Watches(
			&corev1.Secret{},
			handler.EnqueueRequestsFromMapFunc(mapManagedSecretToServiceAccounts(
				mgr.GetClient(),
				store,
				options.ManagedSecretName,
			)),
			builder.WithPredicates(serviceAccountSecretPredicate()),
		).
		WatchesRawSource(source.Channel(resourceEvents.serviceAccountEvents, &handler.EnqueueRequestForObject{})).
		WithOptions(controllerRuntime.Options{
			MaxConcurrentReconciles: options.MaxConcurrentServiceAccountReconciles,
			NeedLeaderElection:      ptr.To(true),
			EnableWarmup:            ptr.To(true),
			UsePriorityQueue:        ptr.To(true),
		}).
		Complete(serviceAccountReconciler); err != nil {
		return fmt.Errorf("register ServiceAccount controller: %w", err)
	}

	configurationReconciler := &ConfigurationReconciler{
		client:    mgr.GetClient(),
		store:     store,
		observer:  configurationObserver,
		publisher: resourceEvents,
		options:   options,
		recorder:  mgr.GetEventRecorderFor("configuration"),
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

func mapManagedSecretToNamespace(name string) handler.MapFunc {
	return func(_ context.Context, object client.Object) []reconcile.Request {
		secret, ok := object.(*corev1.Secret)
		if !ok || secret.Name != name {
			return nil
		}
		return requestForNamespace(secret.Namespace)
	}
}

func mapManagedSecretToServiceAccounts(
	reader client.Reader,
	store *config.Store,
	name string,
) handler.MapFunc {
	return func(ctx context.Context, object client.Object) []reconcile.Request {
		secret, ok := object.(*corev1.Secret)
		if !ok || secret.Name != name || secret.Namespace == "" {
			return nil
		}

		serviceAccounts := &corev1.ServiceAccountList{}
		if err := reader.List(ctx, serviceAccounts, client.InNamespace(secret.Namespace)); err != nil {
			if ctx.Err() != nil && errors.Is(err, context.Canceled) {
				return nil
			}
			log.FromContext(ctx).Error(
				err,
				"list ServiceAccounts after managed Secret creation",
				"namespace", secret.Namespace,
			)
			return nil
		}

		requests := make([]reconcile.Request, 0, len(serviceAccounts.Items))
		for i := range serviceAccounts.Items {
			serviceAccount := &serviceAccounts.Items[i]
			if store.MatchesServiceAccount(serviceAccount.Namespace, serviceAccount.Name) {
				requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(serviceAccount)})
			}
		}
		return requests
	}
}

func serviceAccountSecretPredicate() predicate.Funcs {
	return predicate.Funcs{
		CreateFunc: func(event.CreateEvent) bool { return true },
		DeleteFunc: func(event.DeleteEvent) bool { return false },
		UpdateFunc: func(update event.UpdateEvent) bool {
			oldSecret, oldOK := update.ObjectOld.(*corev1.Secret)
			newSecret, newOK := update.ObjectNew.(*corev1.Secret)
			return oldOK && newOK && registrysecret.IsManaged(oldSecret) != registrysecret.IsManaged(newSecret)
		},
		GenericFunc: func(event.GenericEvent) bool { return false },
	}
}

func requestForNamespace(namespace string) []reconcile.Request {
	if namespace == "" {
		return nil
	}
	return []reconcile.Request{{NamespacedName: types.NamespacedName{Name: namespace}}}
}
