package controller

import (
	"context"
	"errors"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/RyanWang945/kubernetes-registry-secret-controller/internal/config"
	"github.com/RyanWang945/kubernetes-registry-secret-controller/internal/serviceaccount"
)

const (
	secretDependencyInitialDelay = 100 * time.Millisecond
	secretDependencyMaxDelay     = 5 * time.Second
)

// ServiceAccountSyncer owns idempotent imagePullSecrets convergence for one
// ServiceAccount. It must handle both desired targets and cleanup requests.
type ServiceAccountSyncer interface {
	SyncServiceAccount(ctx context.Context, key types.NamespacedName) error
}

// ServiceAccountReconciler preserves the namespaced object key instead of
// expanding one ServiceAccount event into a namespace-wide reconciliation.
type ServiceAccountReconciler struct {
	store  *config.Store
	syncer ServiceAccountSyncer
	// Keep dependency waits separate from the controller queue's error retries:
	// controller-runtime forgets that queue's backoff on every RequeueAfter.
	dependencyBackoff workqueue.TypedRateLimiter[types.NamespacedName]
}

func newServiceAccountReconciler(store *config.Store, syncer ServiceAccountSyncer) *ServiceAccountReconciler {
	return &ServiceAccountReconciler{
		store:  store,
		syncer: syncer,
		dependencyBackoff: workqueue.NewTypedItemExponentialFailureRateLimiter[types.NamespacedName](
			secretDependencyInitialDelay, secretDependencyMaxDelay,
		),
	}
}

func (r *ServiceAccountReconciler) Reconcile(ctx context.Context, request ctrl.Request) (ctrl.Result, error) {
	if request.Namespace == "" || request.Name == "" || !r.store.Loaded() {
		r.dependencyBackoff.Forget(request.NamespacedName)
		return ctrl.Result{}, nil
	}
	if err := r.syncer.SyncServiceAccount(ctx, request.NamespacedName); err != nil {
		if errors.Is(err, serviceaccount.ErrManagedSecretNotReady) {
			delay := r.dependencyBackoff.When(request.NamespacedName)
			log.FromContext(ctx).V(1).Info("waiting for managed Secret", "operation", "sync_serviceaccount", "reason", "ManagedSecretNotReady", "retry_after", delay.String())
			return ctrl.Result{RequeueAfter: delay}, nil
		}
		if ctx.Err() != nil && errors.Is(err, context.Canceled) {
			r.dependencyBackoff.Forget(request.NamespacedName)
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("sync ServiceAccount %q: %w", request.NamespacedName, err)
	}
	// Successful sync includes deleted accounts and accounts leaving the target
	// configuration, so their per-object retry state does not accumulate.
	r.dependencyBackoff.Forget(request.NamespacedName)
	return ctrl.Result{}, nil
}
