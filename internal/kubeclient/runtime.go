package kubeclient

import (
	"context"
	"fmt"
	"sync"

	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/RyanWang945/kubernetes-registry-secret-controller/internal/config"
)

// Settings contains resolved values; legacy flags can still set the two worker
// counts separately when ConfigMap workers is omitted.
type Settings struct {
	QPS                   float64
	Burst                 int
	NamespaceWorkers      int
	ServiceAccountWorkers int
}

func (s Settings) resolve(overrides config.RuntimeConfiguration) (Settings, error) {
	if overrides.KubeAPIQPS != 0 {
		s.QPS = overrides.KubeAPIQPS
	}
	if overrides.KubeAPIBurst != 0 {
		s.Burst = overrides.KubeAPIBurst
	}
	if overrides.Workers != 0 {
		s.NamespaceWorkers, s.ServiceAccountWorkers = overrides.Workers, overrides.Workers
	}
	if err := config.ValidateQPS(s.QPS); err != nil {
		return Settings{}, err
	}
	if s.Burst < 1 || s.NamespaceWorkers < 1 || s.ServiceAccountWorkers < 1 {
		return Settings{}, fmt.Errorf("burst and worker counts must be positive integers")
	}
	return s, nil
}

// Runtime is initialized before Manager creation and updated by the always-on
// configuration controller. Its limiter is only installed on Manager.GetClient;
// informer, discovery, Event, metrics-auth and Lease clients stay independent.
type Runtime struct {
	mu       sync.RWMutex
	fallback Settings
	current  Settings
	desired  Settings
	limiter  *limiter
}

func NewRuntime(fallback Settings, initial config.RuntimeConfiguration) (*Runtime, error) {
	if _, err := fallback.resolve(config.RuntimeConfiguration{}); err != nil {
		return nil, err
	}
	resolved, err := fallback.resolve(initial)
	if err != nil {
		return nil, err
	}
	return &Runtime{fallback: fallback, current: resolved, desired: resolved, limiter: newLimiter(resolved.QPS, resolved.Burst)}, nil
}

// NewClient is a Manager NewClient hook. Copying the REST config preserves the
// independent budgets of every other Manager client. Cache reads remain cached.
func (r *Runtime) NewClient(cfg *rest.Config, options client.Options) (client.Client, error) {
	copy := rest.CopyConfig(cfg)
	copy.RateLimiter = r.limiter
	return client.New(copy, options)
}

func (r *Runtime) Current() Settings {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.current
}

// Apply is idempotent. Only QPS/Burst change in place; worker changes are
// reported with actual and desired counts and require a new process.
func (r *Runtime) Apply(ctx context.Context, overrides config.RuntimeConfiguration) error {
	next, err := r.fallback.resolve(overrides)
	if err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	logger := log.FromContext(ctx)
	if next.QPS != r.current.QPS || next.Burst != r.current.Burst {
		r.limiter.set(next.QPS, next.Burst)
		logger.Info("updated Kubernetes business client rate limit", "operation", "configure_client", "old_qps", r.current.QPS, "old_burst", r.current.Burst, "qps", next.QPS, "burst", next.Burst)
		r.current.QPS, r.current.Burst = next.QPS, next.Burst
	}
	if next.NamespaceWorkers != r.desired.NamespaceWorkers || next.ServiceAccountWorkers != r.desired.ServiceAccountWorkers {
		restart := next.NamespaceWorkers != r.current.NamespaceWorkers || next.ServiceAccountWorkers != r.current.ServiceAccountWorkers
		message := "worker configuration requires restart"
		if !restart {
			message = "worker configuration matches running workers"
		}
		logger.Info(message, "operation", "configure_workers", "restart_required", restart,
			"active_namespace_workers", r.current.NamespaceWorkers, "desired_namespace_workers", next.NamespaceWorkers,
			"active_service_account_workers", r.current.ServiceAccountWorkers, "desired_service_account_workers", next.ServiceAccountWorkers)
	}
	r.desired = next
	return nil
}
