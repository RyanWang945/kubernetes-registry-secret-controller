package controller

import (
	"context"
	"errors"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/RyanWang945/kubernetes-registry-secret-controller/internal/config"
	"github.com/RyanWang945/kubernetes-registry-secret-controller/internal/serviceaccount"
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
}

func (r *ServiceAccountReconciler) Reconcile(ctx context.Context, request ctrl.Request) (ctrl.Result, error) {
	if request.Namespace == "" || request.Name == "" || !r.store.Loaded() {
		return ctrl.Result{}, nil
	}
	if err := r.syncer.SyncServiceAccount(ctx, request.NamespacedName); err != nil {
		if errors.Is(err, serviceaccount.ErrManagedSecretNotReady) {
			log.FromContext(ctx).V(1).Info("waiting for managed Secret", "operation", "sync_serviceaccount", "reason", "ManagedSecretNotReady")
			return ctrl.Result{RequeueAfter: 500 * time.Millisecond}, nil
		}
		if ctx.Err() != nil && errors.Is(err, context.Canceled) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("sync ServiceAccount %q: %w", request.NamespacedName, err)
	}
	return ctrl.Result{}, nil
}
