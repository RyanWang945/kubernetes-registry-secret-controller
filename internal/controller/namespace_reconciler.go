package controller

import (
	"context"
	"fmt"

	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/RyanWang945/kubernetes-registry-secret-controller/internal/config"
)

// NamespaceSyncer owns idempotent resource-level convergence for one namespace.
// It reads the latest configuration and credential snapshots when called and
// must handle both desired targets and cleanup requests; requests do not carry
// a copy of either state.
type NamespaceSyncer interface {
	SyncNamespace(ctx context.Context, namespace string) error
}

// NamespaceReconciler converts controller-runtime requests into the narrow
// domain-level NamespaceSyncer contract.
type NamespaceReconciler struct {
	store  *config.Store
	syncer NamespaceSyncer
}

func (r *NamespaceReconciler) Reconcile(ctx context.Context, request ctrl.Request) (ctrl.Result, error) {
	if request.Namespace != "" || request.Name == "" || !r.store.Loaded() {
		return ctrl.Result{}, nil
	}
	if err := r.syncer.SyncNamespace(ctx, request.Name); err != nil {
		return ctrl.Result{}, fmt.Errorf("sync namespace %q: %w", request.Name, err)
	}
	return ctrl.Result{}, nil
}
