package controller

import (
	"context"
	"log/slog"
)

// LoggingSyncer is the first-milestone implementation. It proves that List,
// Watch, event mapping and queue processing work without claiming to mutate
// Secrets before the resource reconciliation milestone is implemented.
type LoggingSyncer struct {
	logger *slog.Logger
}

func NewLoggingSyncer(logger *slog.Logger) *LoggingSyncer {
	if logger == nil {
		logger = slog.Default()
	}
	return &LoggingSyncer{logger: logger.With("component", "namespace-syncer")}
}

func (s *LoggingSyncer) SyncNamespace(_ context.Context, namespace string) error {
	s.logger.Info("namespace reconciliation requested", "namespace", namespace)
	return nil
}
