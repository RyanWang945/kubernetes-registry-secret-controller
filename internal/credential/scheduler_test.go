package credential

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	clocktesting "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/RyanWang945/kubernetes-registry-secret-controller/internal/config"
)

var testRegistryKey = config.RegistryKey{RegionID: "cn-hangzhou", InstanceID: "cri-test"}

func TestSchedulerRefreshesAtExpirationMinusFiveMinutes(t *testing.T) {
	now := time.Date(2026, time.August, 29, 10, 0, 0, 0, time.UTC)
	fakeClock := clocktesting.NewFakeClock(now)
	configStore := configuredStore(testRegistry("access-key", "access-secret", "registry.example.com"))
	credentialStore := &Store{}
	provider := newRecordingTokenProvider(func(_ context.Context, _ Request, call int) (Token, error) {
		return Token{
			Username:  fmt.Sprintf("user-%d", call),
			Password:  fmt.Sprintf("password-%d", call),
			ExpiresAt: fakeClock.Now().Add(time.Hour),
		}, nil
	})
	publisher := newRecordingNamespacePublisher(0)
	scheduler := mustScheduler(t, configStore, credentialStore, provider, publisher, fakeClock)
	if !scheduler.NeedLeaderElection() {
		t.Fatal("Scheduler must require leader election")
	}
	startScheduler(t, scheduler)

	waitForProviderCalls(t, provider, 1)
	waitForPublisherCalls(t, publisher, 1)
	waitForCredential(t, credentialStore, testRegistryKey, func(entry Entry) bool {
		return entry.Revision == 1 && entry.Credential.Password == "password-1"
	})
	waitForFakeClockWaiters(t, fakeClock, 2)

	fakeClock.Step(54*time.Minute + 59*time.Second)
	assertProviderCallsRemain(t, provider, 1)
	fakeClock.Step(time.Second)
	waitForProviderCalls(t, provider, 2)
	waitForPublisherCalls(t, publisher, 2)
	waitForCredential(t, credentialStore, testRegistryKey, func(entry Entry) bool {
		return entry.Revision == 2 &&
			entry.Credential.Password == "password-2" &&
			entry.Credential.RefreshedAt.Equal(now.Add(55*time.Minute))
	})
}

func TestSchedulerCapsProviderRetryBackoffAtSixtySeconds(t *testing.T) {
	fakeClock := clocktesting.NewFakeClock(time.Date(2026, time.August, 29, 10, 0, 0, 0, time.UTC))
	provider := newRecordingTokenProvider(func(_ context.Context, _ Request, call int) (Token, error) {
		if call <= 5 {
			return Token{}, errors.New("temporary ACR failure")
		}
		return Token{Username: "user", Password: "password", ExpiresAt: fakeClock.Now().Add(time.Hour)}, nil
	})
	publisher := newRecordingNamespacePublisher(0)
	scheduler := mustScheduler(t, configuredStore(testRegistry("key", "secret", "registry.example.com")), &Store{}, provider, publisher, fakeClock)
	startScheduler(t, scheduler)

	waitForRetry(t, scheduler, provider, 1)
	for call, delay := range []time.Duration{5 * time.Second, 10 * time.Second, 20 * time.Second, 40 * time.Second} {
		fakeClock.Step(delay)
		waitForRetry(t, scheduler, provider, call+2)
	}

	// The fifth failure would be 80 seconds without the configured cap.
	fakeClock.Step(59 * time.Second)
	assertProviderCallsRemain(t, provider, 5)
	fakeClock.Step(time.Second)
	waitForProviderCalls(t, provider, 6)
	waitForPublisherCalls(t, publisher, 1)
}

func TestSchedulerRetriesNamespacePublishingWithoutCallingACRAgain(t *testing.T) {
	fakeClock := clocktesting.NewFakeClock(time.Date(2026, time.August, 29, 10, 0, 0, 0, time.UTC))
	provider := newRecordingTokenProvider(func(_ context.Context, _ Request, _ int) (Token, error) {
		return Token{Username: "user", Password: "password", ExpiresAt: fakeClock.Now().Add(time.Hour)}, nil
	})
	publisher := newRecordingNamespacePublisher(1)
	credentialStore := &Store{}
	scheduler := mustScheduler(t, configuredStore(testRegistry("key", "secret", "registry.example.com")), credentialStore, provider, publisher, fakeClock)
	startScheduler(t, scheduler)

	waitForProviderCalls(t, provider, 1)
	waitForPublisherCalls(t, publisher, 1)
	waitForCredential(t, credentialStore, testRegistryKey, func(entry Entry) bool {
		return entry.Credential.Password == "password"
	})
	waitForQueueRequeues(t, scheduler, testRegistryKey, 1)
	waitForFakeClockWaiters(t, fakeClock, 2)

	fakeClock.Step(DefaultRetryBaseDelay)
	waitForPublisherCalls(t, publisher, 2)
	assertProviderCallsRemain(t, provider, 1)
}

func TestSchedulerRefreshesImmediatelyAfterAccessKeyRotation(t *testing.T) {
	fakeClock := clocktesting.NewFakeClock(time.Date(2026, time.August, 29, 10, 0, 0, 0, time.UTC))
	provider := newRecordingTokenProvider(func(_ context.Context, request Request, _ int) (Token, error) {
		return Token{
			Username:  "user",
			Password:  request.AccessKeyID + "-password",
			ExpiresAt: fakeClock.Now().Add(time.Hour),
		}, nil
	})
	configStore := configuredStore(testRegistry("old-key", "old-secret", "registry.example.com"))
	credentialStore := &Store{}
	scheduler := mustScheduler(t, configStore, credentialStore, provider, newRecordingNamespacePublisher(0), fakeClock)
	startScheduler(t, scheduler)

	waitForCredential(t, credentialStore, testRegistryKey, func(entry Entry) bool {
		return entry.Credential.Password == "old-key-password"
	})
	configStore.Apply(configuration(testRegistry("new-key", "new-secret", "registry.example.com")))
	scheduler.NotifyConfigurationChanged()

	waitForProviderCalls(t, provider, 2)
	waitForCredential(t, credentialStore, testRegistryKey, func(entry Entry) bool {
		return entry.Credential.Password == "new-key-password"
	})
}

func TestSchedulerDiscardsCredentialFromRotatedAccessKey(t *testing.T) {
	fakeClock := clocktesting.NewFakeClock(time.Date(2026, time.August, 29, 10, 0, 0, 0, time.UTC))
	firstRelease := make(chan struct{})
	secondRelease := make(chan struct{})
	provider := newRecordingTokenProvider(func(ctx context.Context, request Request, call int) (Token, error) {
		var release <-chan struct{}
		switch call {
		case 1:
			release = firstRelease
		case 2:
			release = secondRelease
		default:
			return Token{}, fmt.Errorf("unexpected provider call %d", call)
		}
		select {
		case <-release:
			return Token{
				Username:  "user",
				Password:  request.AccessKeyID + "-password",
				ExpiresAt: fakeClock.Now().Add(time.Hour),
			}, nil
		case <-ctx.Done():
			return Token{}, ctx.Err()
		}
	})
	configStore := configuredStore(testRegistry("old-key", "old-secret", "registry.example.com"))
	credentialStore := &Store{}
	publisher := newRecordingNamespacePublisher(0)
	scheduler := mustScheduler(t, configStore, credentialStore, provider, publisher, fakeClock)
	startScheduler(t, scheduler)

	firstRequest := waitForProviderRequest(t, provider)
	if firstRequest.AccessKeyID != "old-key" {
		t.Fatalf("first request access key = %q, want old-key", firstRequest.AccessKeyID)
	}
	configStore.Apply(configuration(testRegistry("new-key", "new-secret", "registry.example.com")))
	scheduler.NotifyConfigurationChanged()
	close(firstRelease)

	secondRequest := waitForProviderRequest(t, provider)
	if secondRequest.AccessKeyID != "new-key" || secondRequest.AccessKeySecret != "new-secret" {
		t.Fatalf("second request = %+v, want rotated AK/SK", secondRequest)
	}
	if _, found := credentialStore.Load(testRegistryKey); found {
		t.Fatal("credential returned by obsolete AK/SK was installed")
	}
	close(secondRelease)

	waitForCredential(t, credentialStore, testRegistryKey, func(entry Entry) bool {
		return entry.Credential.Password == "new-key-password"
	})
	waitForPublisherCalls(t, publisher, 1)
}

func TestSchedulerConfigurationDiffActions(t *testing.T) {
	fakeClock := clocktesting.NewFakeClock(time.Date(2026, time.August, 29, 10, 0, 0, 0, time.UTC))
	configStore := configuredStore(testRegistry("key", "secret", "first.example.com"))
	credentialStore := &Store{}
	scheduler := mustScheduler(
		t,
		configStore,
		credentialStore,
		newRecordingTokenProvider(nil),
		newRecordingNamespacePublisher(0),
		fakeClock,
	)
	defer scheduler.queue.ShutDown()
	ctx := log.IntoContext(context.Background(), logr.Discard())

	scheduler.reconcileConfiguration(ctx)
	if scheduler.queue.Len() != 1 {
		t.Fatalf("new Registry queue length = %d, want 1", scheduler.queue.Len())
	}
	consumeQueuedKey(t, scheduler, testRegistryKey)

	configStore.Apply(configuration(testRegistry("key", "secret", "second.example.com")))
	scheduler.reconcileConfiguration(ctx)
	if scheduler.queue.Len() != 0 {
		t.Fatalf("Domain-only change queue length = %d, want 0 ACR work", scheduler.queue.Len())
	}

	configStore.Apply(configuration(testRegistry("rotated-key", "rotated-secret", "second.example.com")))
	scheduler.reconcileConfiguration(ctx)
	if scheduler.queue.Len() != 1 {
		t.Fatalf("AK/SK rotation queue length = %d, want 1", scheduler.queue.Len())
	}
	consumeQueuedKey(t, scheduler, testRegistryKey)

	credentialStore.Apply(
		testRegistry("rotated-key", "rotated-secret", "second.example.com"),
		Credential{Username: "user", Password: "password"},
	)
	configStore.Apply(configuration())
	scheduler.reconcileConfiguration(ctx)
	if _, found := credentialStore.Load(testRegistryKey); found {
		t.Fatal("removed Registry credential remains in Store")
	}
}

func TestSchedulerLimitsConcurrentProviderCallsToTwo(t *testing.T) {
	fakeClock := clocktesting.NewFakeClock(time.Date(2026, time.August, 29, 10, 0, 0, 0, time.UTC))
	release := make(chan struct{})
	provider := newRecordingTokenProvider(func(ctx context.Context, request Request, _ int) (Token, error) {
		select {
		case <-release:
			return Token{
				Username:  "user",
				Password:  request.Key.InstanceID + "-password",
				ExpiresAt: fakeClock.Now().Add(time.Hour),
			}, nil
		case <-ctx.Done():
			return Token{}, ctx.Err()
		}
	})
	registries := []config.RegistryConfig{
		testRegistryForKey(config.RegistryKey{RegionID: "cn-hangzhou", InstanceID: "cri-one"}),
		testRegistryForKey(config.RegistryKey{RegionID: "cn-shanghai", InstanceID: "cri-two"}),
		testRegistryForKey(config.RegistryKey{RegionID: "cn-beijing", InstanceID: "cri-three"}),
	}
	credentialStore := &Store{}
	scheduler := mustScheduler(t, configuredStore(registries...), credentialStore, provider, newRecordingNamespacePublisher(0), fakeClock)
	startScheduler(t, scheduler)

	waitForProviderCalls(t, provider, 2)
	assertProviderCallsRemain(t, provider, 2)
	if got := provider.maximumConcurrency(); got != DefaultWorkerCount {
		t.Fatalf("maximum provider concurrency = %d, want %d", got, DefaultWorkerCount)
	}
	close(release)

	waitForProviderCalls(t, provider, 3)
	eventually(t, func() bool { return len(credentialStore.Snapshot()) == 3 }, "three credentials were not installed")
	if got := provider.maximumConcurrency(); got > DefaultWorkerCount {
		t.Fatalf("maximum provider concurrency = %d, exceeds %d workers", got, DefaultWorkerCount)
	}
}

func TestValidateTokenRequiresUsableRefreshWindow(t *testing.T) {
	t.Parallel()
	now := time.Now()
	tests := []Token{
		{Password: "password", ExpiresAt: now.Add(time.Hour)},
		{Username: "user", ExpiresAt: now.Add(time.Hour)},
		{Username: "user", Password: "password", ExpiresAt: now.Add(DefaultRefreshBefore)},
	}
	for _, token := range tests {
		if err := validateToken(token, now, DefaultRefreshBefore); err == nil {
			t.Fatalf("validateToken(%+v) error = nil, want error", token)
		}
	}
}

func configuredStore(registries ...config.RegistryConfig) *config.Store {
	store := &config.Store{}
	store.Apply(configuration(registries...))
	return store
}

func configuration(registries ...config.RegistryConfig) config.ConfigurationSnapshot {
	snapshot := config.ConfigurationSnapshot{
		Namespaces:      config.NameSelector{MatchAll: true},
		ServiceAccounts: config.NameSelector{MatchAll: true},
		Registries:      make(map[config.RegistryKey]config.RegistryConfig, len(registries)),
	}
	for _, registry := range registries {
		snapshot.Registries[registry.Key()] = registry
	}
	return snapshot
}

func testRegistry(accessKeyID, accessKeySecret, domain string) config.RegistryConfig {
	return config.RegistryConfig{
		RegionID:        testRegistryKey.RegionID,
		InstanceID:      testRegistryKey.InstanceID,
		AccessKeyID:     accessKeyID,
		AccessKeySecret: accessKeySecret,
		Domains:         []string{domain},
	}
}

func testRegistryForKey(key config.RegistryKey) config.RegistryConfig {
	return config.RegistryConfig{
		RegionID:        key.RegionID,
		InstanceID:      key.InstanceID,
		AccessKeyID:     key.InstanceID + "-key",
		AccessKeySecret: key.InstanceID + "-secret",
		Domains:         []string{key.InstanceID + ".example.com"},
	}
}

func mustScheduler(
	t *testing.T,
	configStore *config.Store,
	credentialStore *Store,
	provider TokenProvider,
	publisher NamespaceEventPublisher,
	fakeClock *clocktesting.FakeClock,
) *Scheduler {
	t.Helper()
	scheduler, err := NewScheduler(configStore, credentialStore, provider, publisher, fakeClock, SchedulerOptions{})
	if err != nil {
		t.Fatalf("NewScheduler() error = %v", err)
	}
	return scheduler
}

func startScheduler(t *testing.T, scheduler *Scheduler) {
	t.Helper()
	ctx, cancel := context.WithCancel(log.IntoContext(context.Background(), logr.Discard()))
	done := make(chan error, 1)
	go func() { done <- scheduler.Start(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("Scheduler.Start() error = %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("Scheduler did not stop within five seconds")
		}
	})
}

func consumeQueuedKey(t *testing.T, scheduler *Scheduler, expected config.RegistryKey) {
	t.Helper()
	key, shutdown := scheduler.queue.Get()
	if shutdown || key != expected {
		t.Fatalf("queue.Get() = %v, %v; want %v, false", key, shutdown, expected)
	}
	scheduler.queue.Done(key)
	scheduler.queue.Forget(key)
}

func waitForProviderRequest(t *testing.T, provider *recordingTokenProvider) Request {
	t.Helper()
	select {
	case request := <-provider.started:
		return request
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for provider request")
		return Request{}
	}
}

func waitForProviderCalls(t *testing.T, provider *recordingTokenProvider, calls int) {
	t.Helper()
	eventually(t, func() bool { return provider.callCount() >= calls }, fmt.Sprintf("provider calls did not reach %d", calls))
}

func waitForPublisherCalls(t *testing.T, publisher *recordingNamespacePublisher, calls int) {
	t.Helper()
	eventually(t, func() bool { return publisher.callCount() >= calls }, fmt.Sprintf("publisher calls did not reach %d", calls))
}

func waitForCredential(t *testing.T, store *Store, key config.RegistryKey, matches func(Entry) bool) {
	t.Helper()
	eventually(t, func() bool {
		entry, found := store.Load(key)
		return found && matches(entry)
	}, "credential did not reach expected state")
}

func waitForRetry(t *testing.T, scheduler *Scheduler, provider *recordingTokenProvider, calls int) {
	t.Helper()
	waitForProviderCalls(t, provider, calls)
	waitForQueueRequeues(t, scheduler, testRegistryKey, calls)
	waitForFakeClockWaiters(t, scheduler.clock.(*clocktesting.FakeClock), 2)
}

func waitForQueueRequeues(t *testing.T, scheduler *Scheduler, key config.RegistryKey, requeues int) {
	t.Helper()
	eventually(t, func() bool { return scheduler.queue.NumRequeues(key) == requeues }, fmt.Sprintf("queue requeues did not reach %d", requeues))
}

func waitForFakeClockWaiters(t *testing.T, fakeClock *clocktesting.FakeClock, waiters int) {
	t.Helper()
	eventually(t, func() bool { return fakeClock.Waiters() >= waiters }, fmt.Sprintf("fake clock waiters did not reach %d", waiters))
}

func assertProviderCallsRemain(t *testing.T, provider *recordingTokenProvider, calls int) {
	t.Helper()
	time.Sleep(50 * time.Millisecond)
	if got := provider.callCount(); got != calls {
		t.Fatalf("provider calls = %d, want %d", got, calls)
	}
}

func eventually(t *testing.T, condition func() bool, failureMessage string) {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		if condition() {
			return
		}
		select {
		case <-deadline.C:
			t.Fatal(failureMessage)
		case <-ticker.C:
		}
	}
}

type recordingTokenProvider struct {
	mu        sync.Mutex
	calls     int
	active    int
	maxActive int
	started   chan Request
	result    func(context.Context, Request, int) (Token, error)
}

func newRecordingTokenProvider(result func(context.Context, Request, int) (Token, error)) *recordingTokenProvider {
	return &recordingTokenProvider{started: make(chan Request, 32), result: result}
}

func (p *recordingTokenProvider) GetAuthorizationToken(ctx context.Context, request Request) (Token, error) {
	p.mu.Lock()
	p.calls++
	call := p.calls
	p.active++
	if p.active > p.maxActive {
		p.maxActive = p.active
	}
	result := p.result
	p.mu.Unlock()

	select {
	case p.started <- request:
	case <-ctx.Done():
		p.finishCall()
		return Token{}, ctx.Err()
	}

	var token Token
	var err error
	if result == nil {
		err = errors.New("unexpected provider call")
	} else {
		token, err = result(ctx, request, call)
	}
	p.finishCall()
	return token, err
}

func (p *recordingTokenProvider) finishCall() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.active--
}

func (p *recordingTokenProvider) callCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls
}

func (p *recordingTokenProvider) maximumConcurrency() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.maxActive
}

type recordingNamespacePublisher struct {
	mu        sync.Mutex
	calls     int
	failCalls int
}

func newRecordingNamespacePublisher(failCalls int) *recordingNamespacePublisher {
	return &recordingNamespacePublisher{failCalls: failCalls}
}

func (p *recordingNamespacePublisher) PublishAllNamespaces(context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls++
	if p.calls <= p.failCalls {
		return errors.New("temporary Namespace fan-out failure")
	}
	return nil
}

func (p *recordingNamespacePublisher) callCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls
}
