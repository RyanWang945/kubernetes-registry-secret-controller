package controller

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/RyanWang945/kubernetes-registry-secret-controller/internal/config"
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
		return ctrl.Result{}, fmt.Errorf("sync ServiceAccount %q: %w", request.NamespacedName, err)
	}
	return ctrl.Result{}, nil
}
