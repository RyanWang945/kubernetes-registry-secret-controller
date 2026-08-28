package controller

import (
	"context"

	"sigs.k8s.io/controller-runtime/pkg/log"
)

// LoggingSyncer is the first-milestone implementation. It proves that List,
// Watch, event mapping and queue processing work without claiming to mutate
// Secrets before the resource reconciliation milestone is implemented.
type LoggingSyncer struct{}

func (LoggingSyncer) SyncNamespace(ctx context.Context, namespace string) error {
	log.FromContext(ctx).Info("namespace reconciliation requested", "namespace", namespace)
	return nil
}
