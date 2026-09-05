package main

import (
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"fmt"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"

	"github.com/RyanWang945/kubernetes-registry-secret-controller/internal/controller"
	"github.com/RyanWang945/kubernetes-registry-secret-controller/internal/registrysecret"
)

type phaseKind string

const (
	initialPhase phaseKind = "initial"
	expiryPhase  phaseKind = "expiry"
)

type completionStats struct {
	Count            int     `json:"count"`
	P50Seconds       float64 `json:"p50Seconds"`
	P90Seconds       float64 `json:"p90Seconds"`
	P95Seconds       float64 `json:"p95Seconds"`
	P99Seconds       float64 `json:"p99Seconds"`
	MaximumSeconds   float64 `json:"maximumSeconds"`
	ObjectsPerSecond float64 `json:"objectsPerSecond"`
}

type phaseSummary struct {
	Name                      string          `json:"name"`
	StartedAt                 time.Time       `json:"startedAt"`
	CompletedAt               time.Time       `json:"completedAt"`
	DurationSeconds           float64         `json:"durationSeconds"`
	SecretCompletion          completionStats `json:"secretCompletion"`
	ServiceAccountCompletion  completionStats `json:"serviceAccountCompletion"`
	UnexpectedServiceAccounts int             `json:"unexpectedServiceAccountUpdates"`
}

type phaseProgress struct {
	Timestamp                 time.Time
	Elapsed                   time.Duration
	SecretsComplete           int
	SecretsTotal              int
	ServiceAccountsComplete   int
	ServiceAccountsTotal      int
	UnexpectedServiceAccounts int
}

type trackedPhase struct {
	kind                      phaseKind
	startedAt                 time.Time
	completedAt               time.Time
	secretTotal               int
	serviceAccountTotal       int
	secretDurations           map[string]time.Duration
	secretFingerprints        map[string]string
	serviceAccountDurations   map[string]time.Duration
	unexpectedServiceAccounts map[string]struct{}
	done                      chan struct{}
	doneOnce                  sync.Once
}

type convergenceTracker struct {
	mu sync.Mutex

	targetNamespaces map[string]struct{}
	targetAccounts   map[string]struct{}
	baselineSecrets  map[string]string
	baselineAccounts map[string]string
	current          *trackedPhase
	mutation         *trackedConcurrentMutation
}

func newConvergenceTracker(namespaces []string, serviceAccountNames []string) *convergenceTracker {
	targetNamespaces := make(map[string]struct{}, len(namespaces))
	targetAccounts := make(map[string]struct{}, len(namespaces)*len(serviceAccountNames))
	for _, namespace := range namespaces {
		targetNamespaces[namespace] = struct{}{}
		for _, name := range serviceAccountNames {
			targetAccounts[namespace+"/"+name] = struct{}{}
		}
	}
	return &convergenceTracker{
		targetNamespaces: targetNamespaces,
		targetAccounts:   targetAccounts,
		baselineSecrets:  make(map[string]string, len(namespaces)),
		baselineAccounts: make(map[string]string, len(targetAccounts)),
	}
}

func (t *convergenceTracker) start(kind phaseKind, startedAt time.Time) *trackedPhase {
	t.mu.Lock()
	defer t.mu.Unlock()
	phase := &trackedPhase{
		kind:                      kind,
		startedAt:                 startedAt,
		secretTotal:               len(t.targetNamespaces),
		serviceAccountTotal:       len(t.targetAccounts),
		secretDurations:           make(map[string]time.Duration, len(t.targetNamespaces)),
		secretFingerprints:        make(map[string]string, len(t.targetNamespaces)),
		serviceAccountDurations:   make(map[string]time.Duration, len(t.targetAccounts)),
		unexpectedServiceAccounts: make(map[string]struct{}),
		done:                      make(chan struct{}),
	}
	if kind == expiryPhase {
		phase.serviceAccountTotal = 0
	}
	t.current = phase
	return phase
}

func (t *convergenceTracker) observeSecret(secret *corev1.Secret, observedAt time.Time) {
	if secret == nil || secret.Name != controller.DefaultManagedSecretName {
		return
	}
	if !registrysecret.IsUsableForServiceAccount(secret) {
		return
	}
	fingerprint := secretFingerprint(secret)

	t.mu.Lock()
	defer t.mu.Unlock()
	t.observeMutationSecretLocked(secret.Namespace, fingerprint, observedAt)
	if _, wanted := t.targetNamespaces[secret.Namespace]; !wanted {
		return
	}
	phase := t.current
	if phase == nil || observedAt.Before(phase.startedAt) {
		return
	}

	switch phase.kind {
	case initialPhase:
		t.baselineSecrets[secret.Namespace] = fingerprint
		if _, complete := phase.secretDurations[secret.Namespace]; !complete {
			phase.secretDurations[secret.Namespace] = observedAt.Sub(phase.startedAt)
			phase.secretFingerprints[secret.Namespace] = fingerprint
		}
	case expiryPhase:
		baseline, found := t.baselineSecrets[secret.Namespace]
		if !found || fingerprint == baseline {
			return
		}
		if _, complete := phase.secretDurations[secret.Namespace]; !complete {
			phase.secretDurations[secret.Namespace] = observedAt.Sub(phase.startedAt)
			phase.secretFingerprints[secret.Namespace] = fingerprint
		}
	}
	t.maybeCompleteLocked(phase, observedAt)
}

func (t *convergenceTracker) observeServiceAccount(serviceAccount *corev1.ServiceAccount, observedAt time.Time) {
	if serviceAccount == nil {
		return
	}
	key := serviceAccount.Namespace + "/" + serviceAccount.Name

	t.mu.Lock()
	defer t.mu.Unlock()
	t.observeMutationServiceAccountLocked(serviceAccount, observedAt)
	if _, wanted := t.targetAccounts[key]; !wanted {
		return
	}
	phase := t.current
	if phase == nil || observedAt.Before(phase.startedAt) {
		return
	}

	switch phase.kind {
	case initialPhase:
		if !referencesManagedSecret(serviceAccount) {
			return
		}
		t.baselineAccounts[key] = serviceAccount.ResourceVersion
		if _, complete := phase.serviceAccountDurations[key]; !complete {
			phase.serviceAccountDurations[key] = observedAt.Sub(phase.startedAt)
		}
	case expiryPhase:
		baseline, found := t.baselineAccounts[key]
		if found && serviceAccount.ResourceVersion != baseline {
			phase.unexpectedServiceAccounts[key] = struct{}{}
		}
	}
	t.maybeCompleteLocked(phase, observedAt)
}

func (t *convergenceTracker) maybeCompleteLocked(phase *trackedPhase, observedAt time.Time) {
	complete := len(phase.secretDurations) == phase.secretTotal
	if phase.kind == initialPhase {
		complete = complete && len(phase.serviceAccountDurations) == phase.serviceAccountTotal
	}
	if !complete {
		return
	}
	phase.doneOnce.Do(func() {
		phase.completedAt = observedAt
		close(phase.done)
	})
}

func (t *convergenceTracker) progress(phase *trackedPhase, now time.Time) phaseProgress {
	t.mu.Lock()
	defer t.mu.Unlock()
	elapsed := now.Sub(phase.startedAt)
	if !phase.completedAt.IsZero() {
		elapsed = phase.completedAt.Sub(phase.startedAt)
	}
	return phaseProgress{
		Timestamp:                 now.UTC(),
		Elapsed:                   elapsed,
		SecretsComplete:           len(phase.secretDurations),
		SecretsTotal:              phase.secretTotal,
		ServiceAccountsComplete:   len(phase.serviceAccountDurations),
		ServiceAccountsTotal:      phase.serviceAccountTotal,
		UnexpectedServiceAccounts: len(phase.unexpectedServiceAccounts),
	}
}

func (t *convergenceTracker) summary(phase *trackedPhase) phaseSummary {
	t.mu.Lock()
	defer t.mu.Unlock()
	completedAt := phase.completedAt
	if completedAt.IsZero() {
		completedAt = time.Now()
	}
	duration := completedAt.Sub(phase.startedAt)
	return phaseSummary{
		Name:                      string(phase.kind),
		StartedAt:                 phase.startedAt.UTC(),
		CompletedAt:               completedAt.UTC(),
		DurationSeconds:           duration.Seconds(),
		SecretCompletion:          calculateStats(phase.secretDurations),
		ServiceAccountCompletion:  calculateStats(phase.serviceAccountDurations),
		UnexpectedServiceAccounts: len(phase.unexpectedServiceAccounts),
	}
}

func (t *convergenceTracker) mutationTriggerCandidate(
	phase *trackedPhase,
) (phaseProgress, string, string, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	progress := phaseProgress{
		Timestamp:               time.Now().UTC(),
		Elapsed:                 time.Since(phase.startedAt),
		SecretsComplete:         len(phase.secretDurations),
		SecretsTotal:            phase.secretTotal,
		ServiceAccountsComplete: len(phase.serviceAccountDurations),
		ServiceAccountsTotal:    phase.serviceAccountTotal,
	}
	if !phase.completedAt.IsZero() {
		progress.Elapsed = phase.completedAt.Sub(phase.startedAt)
	}

	namespace := ""
	for candidate := range phase.secretDurations {
		if namespace == "" || candidate < namespace {
			namespace = candidate
		}
	}
	if namespace == "" {
		return progress, "", "", false
	}
	fingerprint, found := phase.secretFingerprints[namespace]
	return progress, namespace, fingerprint, found
}

func (t *convergenceTracker) startConcurrentMutation(
	triggerPercent int,
	triggeredAt time.Time,
	progress phaseProgress,
	existingNamespace string,
	lateServiceAccountName string,
	lateNamespace string,
	lateNamespaceAccounts []string,
	expectedCredentialFingerprint string,
) *trackedConcurrentMutation {
	t.mu.Lock()
	defer t.mu.Unlock()

	accounts := make(map[string]struct{}, len(lateNamespaceAccounts))
	for _, name := range lateNamespaceAccounts {
		accounts[name] = struct{}{}
	}
	mutation := &trackedConcurrentMutation{
		triggerPercent:                 triggerPercent,
		triggeredAt:                    triggeredAt,
		secretsCompleteAtTrigger:       progress.SecretsComplete,
		secretsTotalAtTrigger:          progress.SecretsTotal,
		existingNamespace:              existingNamespace,
		expectedCredentialFingerprint:  expectedCredentialFingerprint,
		lateServiceAccountName:         lateServiceAccountName,
		lateNamespace:                  lateNamespace,
		lateNamespaceAccounts:          accounts,
		lateNamespaceStaleFingerprints: make(map[string]struct{}),
		lateNamespaceAccountReadyAt:    make(map[string]time.Time, len(accounts)),
		done:                           make(chan struct{}),
	}
	t.mutation = mutation
	return mutation
}

func (t *convergenceTracker) markLateServiceAccountRequestStarted(
	mutation *trackedConcurrentMutation,
	at time.Time,
) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.mutation == mutation {
		mutation.lateServiceAccountRequestStarted = at
	}
}

func (t *convergenceTracker) markLateServiceAccountCreated(
	mutation *trackedConcurrentMutation,
	at time.Time,
) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.mutation == mutation {
		mutation.lateServiceAccountCreatedAt = at
		t.maybeCompleteMutationLocked(mutation)
	}
}

func (t *convergenceTracker) markLateNamespaceRequestStarted(
	mutation *trackedConcurrentMutation,
	at time.Time,
) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.mutation == mutation {
		mutation.lateNamespaceRequestStarted = at
	}
}

func (t *convergenceTracker) markLateNamespaceCreated(
	mutation *trackedConcurrentMutation,
	at time.Time,
) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.mutation == mutation {
		mutation.lateNamespaceCreatedAt = at
		t.maybeCompleteMutationLocked(mutation)
	}
}

func (t *convergenceTracker) markLateNamespacePrerequisitesReady(
	mutation *trackedConcurrentMutation,
	at time.Time,
) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.mutation == mutation {
		mutation.lateNamespacePrerequisitesAt = at
		t.maybeCompleteMutationLocked(mutation)
	}
}

func (t *convergenceTracker) observeMutationSecretLocked(
	namespace string,
	fingerprint string,
	observedAt time.Time,
) {
	mutation := t.mutation
	if mutation == nil || namespace != mutation.lateNamespace {
		return
	}
	if fingerprint != mutation.expectedCredentialFingerprint {
		mutation.lateNamespaceStaleFingerprints[fingerprint] = struct{}{}
		return
	}
	if mutation.lateNamespaceSecretReadyAt.IsZero() {
		mutation.lateNamespaceSecretReadyAt = observedAt
		mutation.lateNamespaceSecretFingerprint = fingerprint
	}
	t.maybeCompleteMutationLocked(mutation)
}

func (t *convergenceTracker) observeMutationServiceAccountLocked(
	serviceAccount *corev1.ServiceAccount,
	observedAt time.Time,
) {
	mutation := t.mutation
	if mutation == nil || !referencesManagedSecret(serviceAccount) {
		return
	}
	if serviceAccount.Namespace == mutation.existingNamespace &&
		serviceAccount.Name == mutation.lateServiceAccountName &&
		mutation.lateServiceAccountReadyAt.IsZero() {
		mutation.lateServiceAccountReadyAt = observedAt
	}
	if serviceAccount.Namespace == mutation.lateNamespace {
		if _, wanted := mutation.lateNamespaceAccounts[serviceAccount.Name]; wanted {
			if _, found := mutation.lateNamespaceAccountReadyAt[serviceAccount.Name]; !found {
				mutation.lateNamespaceAccountReadyAt[serviceAccount.Name] = observedAt
			}
		}
	}
	t.maybeCompleteMutationLocked(mutation)
}

func (t *convergenceTracker) maybeCompleteMutationLocked(mutation *trackedConcurrentMutation) {
	if mutation.lateServiceAccountCreatedAt.IsZero() ||
		mutation.lateNamespaceCreatedAt.IsZero() ||
		mutation.lateNamespacePrerequisitesAt.IsZero() ||
		mutation.lateServiceAccountReadyAt.IsZero() ||
		mutation.lateNamespaceSecretReadyAt.IsZero() ||
		len(mutation.lateNamespaceAccountReadyAt) != len(mutation.lateNamespaceAccounts) {
		return
	}
	mutation.doneOnce.Do(func() { close(mutation.done) })
}

func (t *convergenceTracker) concurrentMutationSummary(
	mutation *trackedConcurrentMutation,
) concurrentMutationSummary {
	t.mu.Lock()
	defer t.mu.Unlock()

	serviceAccountsReadyAt := time.Time{}
	for _, readyAt := range mutation.lateNamespaceAccountReadyAt {
		serviceAccountsReadyAt = laterTime(serviceAccountsReadyAt, readyAt)
	}
	readyAt := laterTime(mutation.lateNamespaceSecretReadyAt, serviceAccountsReadyAt)
	return concurrentMutationSummary{
		TriggerPercent:                mutation.triggerPercent,
		TriggeredAt:                   mutation.triggeredAt.UTC(),
		SecretsCompleteAtTrigger:      mutation.secretsCompleteAtTrigger,
		SecretsTotalAtTrigger:         mutation.secretsTotalAtTrigger,
		ExistingNamespace:             mutation.existingNamespace,
		ExpectedCredentialFingerprint: mutation.expectedCredentialFingerprint,
		LateServiceAccount: mutationResourceSummary{
			Namespace:            mutation.existingNamespace,
			Name:                 mutation.lateServiceAccountName,
			CreateRequestStarted: mutation.lateServiceAccountRequestStarted.UTC(),
			CreatedAt:            mutation.lateServiceAccountCreatedAt.UTC(),
			ReadyAt:              mutation.lateServiceAccountReadyAt.UTC(),
			ReadyLatencySeconds:  mutationLatency(mutation.lateServiceAccountCreatedAt, mutation.lateServiceAccountReadyAt),
		},
		LateNamespace: mutationNamespaceSummary{
			Name:                           mutation.lateNamespace,
			CreateRequestStarted:           mutation.lateNamespaceRequestStarted.UTC(),
			CreatedAt:                      mutation.lateNamespaceCreatedAt.UTC(),
			PrerequisitesReadyAt:           mutation.lateNamespacePrerequisitesAt.UTC(),
			SecretReadyAt:                  mutation.lateNamespaceSecretReadyAt.UTC(),
			ServiceAccountsReadyAt:         serviceAccountsReadyAt.UTC(),
			ReadyAt:                        readyAt.UTC(),
			SecretLatencySeconds:           mutationLatency(mutation.lateNamespaceCreatedAt, mutation.lateNamespaceSecretReadyAt),
			ServiceAccountsLatencySeconds:  mutationLatency(mutation.lateNamespaceCreatedAt, serviceAccountsReadyAt),
			ReadyLatencySeconds:            mutationLatency(mutation.lateNamespaceCreatedAt, readyAt),
			PrerequisitesToReadySeconds:    mutationLatency(mutation.lateNamespacePrerequisitesAt, readyAt),
			ObservedCredentialFingerprint:  mutation.lateNamespaceSecretFingerprint,
			StaleCredentialFingerprints:    len(mutation.lateNamespaceStaleFingerprints),
			ExpectedServiceAccountCount:    len(mutation.lateNamespaceAccounts),
			ObservedPatchedServiceAccounts: len(mutation.lateNamespaceAccountReadyAt),
		},
	}
}

func calculateStats(values map[string]time.Duration) completionStats {
	durations := make([]float64, 0, len(values))
	for _, duration := range values {
		durations = append(durations, duration.Seconds())
	}
	sort.Float64s(durations)
	stats := completionStats{Count: len(durations)}
	if len(durations) == 0 {
		return stats
	}
	stats.P50Seconds = percentile(durations, 0.50)
	stats.P90Seconds = percentile(durations, 0.90)
	stats.P95Seconds = percentile(durations, 0.95)
	stats.P99Seconds = percentile(durations, 0.99)
	stats.MaximumSeconds = durations[len(durations)-1]
	if stats.MaximumSeconds > 0 {
		stats.ObjectsPerSecond = float64(len(durations)) / stats.MaximumSeconds
	}
	return stats
}

func percentile(sorted []float64, quantile float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	if len(sorted) == 1 {
		return sorted[0]
	}
	position := quantile * float64(len(sorted)-1)
	lower := int(position)
	upper := lower + 1
	if upper >= len(sorted) {
		return sorted[lower]
	}
	fraction := position - float64(lower)
	return sorted[lower] + (sorted[upper]-sorted[lower])*fraction
}

func referencesManagedSecret(serviceAccount *corev1.ServiceAccount) bool {
	for _, reference := range serviceAccount.ImagePullSecrets {
		if reference.Name == controller.DefaultManagedSecretName {
			return true
		}
	}
	return false
}

func secretFingerprint(secret *corev1.Secret) string {
	hash := sha256.New()
	hash.Write(secret.Data[corev1.DockerConfigJsonKey])
	hash.Write([]byte{0})
	hash.Write([]byte(secret.Annotations[registrysecret.StateAnnotationKey]))
	return hex.EncodeToString(hash.Sum(nil))
}

type progressRecorder struct {
	file   *os.File
	writer *csv.Writer
}

func newProgressRecorder(path string) (*progressRecorder, error) {
	file, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("create progress CSV: %w", err)
	}
	writer := csv.NewWriter(file)
	if err := writer.Write([]string{
		"timestamp",
		"elapsed_seconds",
		"secrets_complete",
		"secrets_total",
		"serviceaccounts_complete",
		"serviceaccounts_total",
		"unexpected_serviceaccount_updates",
	}); err != nil {
		file.Close()
		return nil, fmt.Errorf("write progress CSV header: %w", err)
	}
	writer.Flush()
	return &progressRecorder{file: file, writer: writer}, nil
}

func (r *progressRecorder) Write(progress phaseProgress) error {
	if err := r.writer.Write([]string{
		progress.Timestamp.Format(time.RFC3339Nano),
		strconv.FormatFloat(progress.Elapsed.Seconds(), 'f', 3, 64),
		strconv.Itoa(progress.SecretsComplete),
		strconv.Itoa(progress.SecretsTotal),
		strconv.Itoa(progress.ServiceAccountsComplete),
		strconv.Itoa(progress.ServiceAccountsTotal),
		strconv.Itoa(progress.UnexpectedServiceAccounts),
	}); err != nil {
		return err
	}
	r.writer.Flush()
	return r.writer.Error()
}

func (r *progressRecorder) Close() error {
	r.writer.Flush()
	writerErr := r.writer.Error()
	fileErr := r.file.Close()
	if writerErr != nil {
		return writerErr
	}
	return fileErr
}
