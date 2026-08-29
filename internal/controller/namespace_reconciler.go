package controller

import (
	"context"
	"fmt"

	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/RyanWang945/kubernetes-registry-secret-controller/internal/config"
)

// NamespaceSecretSyncer owns idempotent Secret convergence for one namespace.
// It reads the latest configuration and credential snapshots when called and
// must handle both desired targets and cleanup requests; requests do not carry
// a copy of either state.
type NamespaceSecretSyncer interface {
	SyncNamespaceSecret(ctx context.Context, namespace string) error
}

// NamespaceSecretReconciler converts cluster-scoped Namespace requests into the
// narrow domain-level NamespaceSecretSyncer contract.
type NamespaceSecretReconciler struct {
	store  *config.Store
	syncer NamespaceSecretSyncer
}

func (r *NamespaceSecretReconciler) Reconcile(ctx context.Context, request ctrl.Request) (ctrl.Result, error) {
	if request.Namespace != "" || request.Name == "" || !r.store.Loaded() {
		return ctrl.Result{}, nil
	}
	if err := r.syncer.SyncNamespaceSecret(ctx, request.Name); err != nil {
		return ctrl.Result{}, fmt.Errorf("sync Secret in namespace %q: %w", request.Name, err)
	}
	return ctrl.Result{}, nil
}
