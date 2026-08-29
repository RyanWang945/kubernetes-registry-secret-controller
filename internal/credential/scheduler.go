package credential

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"k8s.io/client-go/util/workqueue"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/RyanWang945/kubernetes-registry-secret-controller/internal/config"
)

const (
	DefaultRefreshBefore  = 5 * time.Minute
	DefaultRequestTimeout = 15 * time.Second
	DefaultWorkerCount    = 2
	DefaultRetryBaseDelay = 5 * time.Second
	DefaultRetryMaxDelay  = 60 * time.Second
	credentialQueueName   = "registry_credentials"
)

// NamespaceEventPublisher schedules Secret convergence after a credential is
// installed. A publishing error is retried without calling TokenProvider again.
type NamespaceEventPublisher interface {
	PublishAllNamespaces(ctx context.Context) error
}

// SchedulerOptions are internal policy knobs. They are intentionally not
// exposed through the ConfigMap or command-line interface; non-default values
// are primarily useful for deterministic tests.
type SchedulerOptions struct {
	RefreshBefore  time.Duration
	RequestTimeout time.Duration
	WorkerCount    int
	RetryBaseDelay time.Duration
	RetryMaxDelay  time.Duration
}

func (o SchedulerOptions) withDefaults() SchedulerOptions {
	if o.RefreshBefore == 0 {
		o.RefreshBefore = DefaultRefreshBefore
	}
	if o.RequestTimeout == 0 {
		o.RequestTimeout = DefaultRequestTimeout
	}
	if o.WorkerCount == 0 {
		o.WorkerCount = DefaultWorkerCount
	}
	if o.RetryBaseDelay == 0 {
		o.RetryBaseDelay = DefaultRetryBaseDelay
	}
	if o.RetryMaxDelay == 0 {
		o.RetryMaxDelay = DefaultRetryMaxDelay
	}
	return o
}

func (o SchedulerOptions) validate() error {
	if o.RefreshBefore <= 0 {
		return errors.New("credential refresh-before duration must be positive")
	}
	if o.RequestTimeout <= 0 {
		return errors.New("credential request timeout must be positive")
	}
	if o.WorkerCount <= 0 {
		return errors.New("credential worker count must be positive")
	}
	if o.RetryBaseDelay <= 0 || o.RetryMaxDelay <= 0 || o.RetryBaseDelay > o.RetryMaxDelay {
		return errors.New("credential retry delays must be positive and base must not exceed maximum")
	}
	return nil
}

// Scheduler is a Manager Runnable that owns one delayed/rate-limited queue for
// all RegistryKeys. It runs only on the elected leader.
type Scheduler struct {
	configStore     *config.Store
	credentialStore *Store
	provider        TokenProvider
	publisher       NamespaceEventPublisher
	clock           clock.WithTicker
	options         SchedulerOptions
	queue           workqueue.TypedRateLimitingInterface[config.RegistryKey]

	configurationChanges chan struct{}
	knownRegistries      map[config.RegistryKey]config.RegistryConfig
	stateMu              sync.Mutex
	publishedRevisions   map[config.RegistryKey]uint64
	forceRefreshRevision map[config.RegistryKey]uint64
}

func NewScheduler(
	configStore *config.Store,
	credentialStore *Store,
	provider TokenProvider,
	publisher NamespaceEventPublisher,
	clock clock.WithTicker,
	options SchedulerOptions,
) (*Scheduler, error) {
	if configStore == nil {
		return nil, errors.New("config store must not be nil")
	}
	if credentialStore == nil {
		return nil, errors.New("credential store must not be nil")
	}
	if provider == nil {
		return nil, errors.New("token provider must not be nil")
	}
	if publisher == nil {
		return nil, errors.New("Namespace event publisher must not be nil")
	}
	if clock == nil {
		return nil, errors.New("clock must not be nil")
	}

	options = options.withDefaults()
	if err := options.validate(); err != nil {
		return nil, err
	}

	rateLimiter := workqueue.NewTypedItemExponentialFailureRateLimiter[config.RegistryKey](
		options.RetryBaseDelay,
		options.RetryMaxDelay,
	)
	return &Scheduler{
		configStore:     configStore,
		credentialStore: credentialStore,
		provider:        provider,
		publisher:       publisher,
		clock:           clock,
		options:         options,
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			rateLimiter,
			workqueue.TypedRateLimitingQueueConfig[config.RegistryKey]{
				Name:  credentialQueueName,
				Clock: clock,
			},
		),
		configurationChanges: make(chan struct{}, 1),
		knownRegistries:      make(map[config.RegistryKey]config.RegistryConfig),
		publishedRevisions:   make(map[config.RegistryKey]uint64),
		forceRefreshRevision: make(map[config.RegistryKey]uint64),
	}, nil
}

// NeedLeaderElection prevents followers from calling ACR or owning refresh
// timers while still allowing their caches and configuration controller to run.
func (*Scheduler) NeedLeaderElection() bool {
	return true
}

// NotifyConfigurationChanged coalesces notifications. Start always compares
// complete snapshots, so no semantic update can be lost.
func (s *Scheduler) NotifyConfigurationChanged() {
	select {
	case s.configurationChanges <- struct{}{}:
	default:
	}
}

func (s *Scheduler) Start(ctx context.Context) error {
	logger := log.FromContext(ctx).WithName("credential-scheduler")
	ctx = log.IntoContext(ctx, logger)
	s.reconcileConfiguration(ctx)

	var workers sync.WaitGroup
	workers.Add(s.options.WorkerCount)
	for range s.options.WorkerCount {
		go func() {
			defer workers.Done()
			s.runWorker(ctx)
		}()
	}

	for {
		select {
		case <-ctx.Done():
			s.queue.ShutDown()
			workers.Wait()
			return nil
		case <-s.configurationChanges:
			s.reconcileConfiguration(ctx)
		}
	}
}

func (s *Scheduler) reconcileConfiguration(ctx context.Context) {
	snapshot, loaded := s.configStore.Load()
	if !loaded {
		return
	}

	logger := log.FromContext(ctx)
	for key, registry := range snapshot.Registries {
		previous, found := s.knownRegistries[key]
		if !found {
			logger.Info("scheduled credential acquisition", "registry", key.String(), "reason", "registry-added")
			s.queue.Add(key)
			continue
		}
		if authenticationChanged(previous, registry) {
			logger.Info("scheduled credential acquisition", "registry", key.String(), "reason", "access-key-changed")
			s.markForceRefresh(key)
			s.queue.Add(key)
		}
	}

	for key := range s.knownRegistries {
		if _, found := snapshot.Registries[key]; found {
			continue
		}
		if s.credentialStore.Delete(key) {
			logger.Info("removed unconfigured credential", "registry", key.String())
		}
		s.forgetPublishedRevision(key)
		s.deleteForceRefresh(key)
		s.queue.Forget(key)
	}

	s.knownRegistries = cloneRegistries(snapshot.Registries)
}

func (s *Scheduler) runWorker(ctx context.Context) {
	for {
		key, shutdown := s.queue.Get()
		if shutdown {
			return
		}

		err := s.refresh(ctx, key)
		s.queue.Done(key)
		if err == nil {
			s.queue.Forget(key)
			continue
		}
		if ctx.Err() != nil {
			s.queue.Forget(key)
			continue
		}

		log.FromContext(ctx).Error(
			err,
			"credential reconciliation failed",
			"registry", key.String(),
			"retry", s.queue.NumRequeues(key)+1,
		)
		s.queue.AddRateLimited(key)
	}
}

func (s *Scheduler) refresh(ctx context.Context, key config.RegistryKey) error {
	registry, configured := s.loadRegistry(key)
	if !configured {
		s.credentialStore.Delete(key)
		s.forgetPublishedRevision(key)
		s.deleteForceRefresh(key)
		return nil
	}

	now := s.clock.Now()
	forceRefreshRevision := s.currentForceRefreshRevision(key)
	if entry, found := s.credentialStore.Load(key); found &&
		entry.MatchesRegistry(registry) && forceRefreshRevision == 0 {
		refreshAt := entry.Credential.ExpiresAt.Add(-s.options.RefreshBefore)
		if refreshAt.After(now) {
			if err := s.publishCredential(ctx, key, entry); err != nil {
				return err
			}
			s.queue.AddAfter(key, refreshAt.Sub(now))
			return nil
		}
	}

	requestCtx, cancel := context.WithTimeout(ctx, s.options.RequestTimeout)
	token, err := s.provider.GetAuthorizationToken(requestCtx, Request{
		Key:             key,
		AccessKeyID:     registry.AccessKeyID,
		AccessKeySecret: registry.AccessKeySecret,
	})
	cancel()
	if err != nil {
		return fmt.Errorf("get authorization token: %w", err)
	}

	current, stillConfigured := s.loadRegistry(key)
	if !stillConfigured {
		s.credentialStore.Delete(key)
		s.forgetPublishedRevision(key)
		s.deleteForceRefresh(key)
		return nil
	}
	if authenticationChanged(registry, current) {
		// The completed response belongs to an obsolete AK/SK. Marking the key
		// dirty causes an immediate retry with the current configuration.
		s.markForceRefresh(key)
		s.queue.Add(key)
		return nil
	}

	now = s.clock.Now()
	if err := validateToken(token, now, s.options.RefreshBefore); err != nil {
		return err
	}
	entry := s.credentialStore.Apply(current, Credential{
		Username:    token.Username,
		Password:    token.Password,
		RefreshedAt: now,
		ExpiresAt:   token.ExpiresAt,
	})
	s.clearForceRefresh(key, forceRefreshRevision)
	log.FromContext(ctx).Info(
		"installed refreshed credential",
		"registry", key.String(),
		"expires_at", token.ExpiresAt,
	)

	if err := s.publishCredential(ctx, key, entry); err != nil {
		return err
	}
	s.queue.AddAfter(key, token.ExpiresAt.Add(-s.options.RefreshBefore).Sub(now))
	return nil
}

func (s *Scheduler) publishCredential(ctx context.Context, key config.RegistryKey, entry Entry) error {
	if s.isPublished(key, entry.Revision) {
		return nil
	}
	if err := s.publisher.PublishAllNamespaces(ctx); err != nil {
		return fmt.Errorf("publish Namespace events for refreshed credential: %w", err)
	}
	s.markPublished(key, entry.Revision)
	return nil
}

func (s *Scheduler) loadRegistry(key config.RegistryKey) (config.RegistryConfig, bool) {
	snapshot, loaded := s.configStore.Load()
	if !loaded {
		return config.RegistryConfig{}, false
	}
	registry, found := snapshot.Registries[key]
	return registry, found
}

func (s *Scheduler) isPublished(key config.RegistryKey, revision uint64) bool {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	return s.publishedRevisions[key] == revision
}

func (s *Scheduler) markPublished(key config.RegistryKey, revision uint64) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	s.publishedRevisions[key] = revision
}

func (s *Scheduler) forgetPublishedRevision(key config.RegistryKey) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	delete(s.publishedRevisions, key)
}

func (s *Scheduler) markForceRefresh(key config.RegistryKey) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	s.forceRefreshRevision[key]++
}

func (s *Scheduler) clearForceRefresh(key config.RegistryKey, handledRevision uint64) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.forceRefreshRevision[key] == handledRevision {
		delete(s.forceRefreshRevision, key)
	}
}

func (s *Scheduler) deleteForceRefresh(key config.RegistryKey) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	delete(s.forceRefreshRevision, key)
}

func (s *Scheduler) currentForceRefreshRevision(key config.RegistryKey) uint64 {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	return s.forceRefreshRevision[key]
}

func validateToken(token Token, now time.Time, refreshBefore time.Duration) error {
	if token.Username == "" {
		return errors.New("provider returned an empty credential username")
	}
	if token.Password == "" {
		return errors.New("provider returned an empty credential password")
	}
	if !token.ExpiresAt.After(now.Add(refreshBefore)) {
		return fmt.Errorf("provider returned a credential that expires inside the %s refresh window", refreshBefore)
	}
	return nil
}

func authenticationChanged(previous, current config.RegistryConfig) bool {
	return previous.AccessKeyID != current.AccessKeyID ||
		previous.AccessKeySecret != current.AccessKeySecret
}

func cloneRegistries(source map[config.RegistryKey]config.RegistryConfig) map[config.RegistryKey]config.RegistryConfig {
	clone := make(map[config.RegistryKey]config.RegistryConfig, len(source))
	for key, registry := range source {
		registry.Domains = append([]string(nil), registry.Domains...)
		clone[key] = registry
	}
	return clone
}
