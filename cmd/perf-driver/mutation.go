package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/RyanWang945/kubernetes-registry-secret-controller/internal/controller"
	"github.com/RyanWang945/kubernetes-registry-secret-controller/internal/registrysecret"
)

const mutationTriggerPollInterval = 10 * time.Millisecond

type mutationResourceSummary struct {
	Namespace            string    `json:"namespace"`
	Name                 string    `json:"name"`
	CreateRequestStarted time.Time `json:"createRequestStartedAt"`
	CreatedAt            time.Time `json:"createdAt"`
	ReadyAt              time.Time `json:"readyAt"`
	ReadyLatencySeconds  float64   `json:"readyLatencySeconds"`
}

type mutationNamespaceSummary struct {
	Name                           string    `json:"name"`
	CreateRequestStarted           time.Time `json:"createRequestStartedAt"`
	CreatedAt                      time.Time `json:"createdAt"`
	PrerequisitesReadyAt           time.Time `json:"prerequisitesReadyAt"`
	SecretReadyAt                  time.Time `json:"secretReadyAt"`
	ServiceAccountsReadyAt         time.Time `json:"serviceAccountsReadyAt"`
	ReadyAt                        time.Time `json:"readyAt"`
	SecretLatencySeconds           float64   `json:"secretLatencySeconds"`
	ServiceAccountsLatencySeconds  float64   `json:"serviceAccountsLatencySeconds"`
	ReadyLatencySeconds            float64   `json:"readyLatencySeconds"`
	PrerequisitesToReadySeconds    float64   `json:"prerequisitesToReadySeconds"`
	ObservedCredentialFingerprint  string    `json:"observedCredentialFingerprint"`
	StaleCredentialFingerprints    int       `json:"staleCredentialFingerprints"`
	ExpectedServiceAccountCount    int       `json:"expectedServiceAccountCount"`
	ObservedPatchedServiceAccounts int       `json:"observedPatchedServiceAccounts"`
}

type concurrentMutationSummary struct {
	NamespaceOnly                 bool                     `json:"namespaceOnly,omitempty"`
	TriggerPercent                int                      `json:"triggerPercent"`
	TriggeredAt                   time.Time                `json:"triggeredAt"`
	SecretsCompleteAtTrigger      int                      `json:"secretsCompleteAtTrigger"`
	SecretsTotalAtTrigger         int                      `json:"secretsTotalAtTrigger"`
	ExistingNamespace             string                   `json:"existingNamespace"`
	ExpectedCredentialFingerprint string                   `json:"expectedCredentialFingerprint"`
	LateServiceAccount            mutationResourceSummary  `json:"lateServiceAccount"`
	LateNamespace                 mutationNamespaceSummary `json:"lateNamespace"`
	Success                       bool                     `json:"success"`
	Error                         string                   `json:"error,omitempty"`
}

type trackedConcurrentMutation struct {
	triggerPercent                int
	triggeredAt                   time.Time
	secretsCompleteAtTrigger      int
	secretsTotalAtTrigger         int
	existingNamespace             string
	expectedCredentialFingerprint string
	lateServiceAccountName        string
	lateNamespace                 string
	lateNamespaceAccounts         map[string]struct{}

	lateServiceAccountRequestStarted time.Time
	lateServiceAccountCreatedAt      time.Time
	lateServiceAccountReadyAt        time.Time
	lateNamespaceRequestStarted      time.Time
	lateNamespaceCreatedAt           time.Time
	lateNamespacePrerequisitesAt     time.Time
	lateNamespaceSecretReadyAt       time.Time
	lateNamespaceSecretFingerprint   string
	lateNamespaceStaleFingerprints   map[string]struct{}
	lateNamespaceAccountReadyAt      map[string]time.Time
	done                             chan struct{}
	doneOnce                         sync.Once
}

type mutationOutcome struct {
	summary concurrentMutationSummary
	err     error
}

func runConcurrentMutation(
	ctx context.Context,
	clientset kubernetes.Interface,
	tracker *convergenceTracker,
	phase *trackedPhase,
	runID string,
	triggerPercent int,
	timeout time.Duration,
	namespaceOnly bool,
) mutationOutcome {
	mutationCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	progress, existingNamespace, fingerprint, err := waitForMutationTrigger(
		mutationCtx,
		tracker,
		phase,
		triggerPercent,
	)
	if err != nil {
		return mutationOutcome{err: err, summary: concurrentMutationSummary{
			TriggerPercent: triggerPercent,
			Error:          err.Error(),
		}}
	}

	triggeredAt := time.Now()
	lateNamespace := concurrentNamespaceName(runID)
	lateAccountName := serviceAccountLate
	if namespaceOnly {
		existingNamespace, lateAccountName = "", ""
	}
	mutation := tracker.startConcurrentMutation(
		triggerPercent,
		triggeredAt,
		progress,
		existingNamespace,
		lateAccountName,
		lateNamespace,
		targetServiceAccounts,
		fingerprint,
	)

	start := make(chan struct{})
	creationErrors := make(chan error, 2)
	creations := 1
	if !namespaceOnly {
		creations++
		go func() {
			<-start
			creationErrors <- createLateServiceAccount(mutationCtx, clientset, tracker, mutation, runID)
		}()
	}
	go func() {
		<-start
		creationErrors <- createLateNamespace(mutationCtx, clientset, tracker, mutation, runID)
	}()
	close(start)

	var creationErr error
	for range creations {
		creationErr = errors.Join(creationErr, <-creationErrors)
	}
	if creationErr != nil {
		summary := tracker.concurrentMutationSummary(mutation)
		summary.Error = creationErr.Error()
		return mutationOutcome{summary: summary, err: creationErr}
	}

	select {
	case <-mutation.done:
		summary := tracker.concurrentMutationSummary(mutation)
		if summary.LateNamespace.StaleCredentialFingerprints != 0 {
			err := fmt.Errorf(
				"injected Namespace exposed %d stale credential fingerprint(s) before the current generation",
				summary.LateNamespace.StaleCredentialFingerprints,
			)
			summary.Error = err.Error()
			return mutationOutcome{summary: summary, err: err}
		}
		summary.Success = true
		fmt.Printf(
			"INJECT_COMPLETE serviceaccount_latency=%.3fs namespace_secret_latency=%.3fs namespace_ready_latency=%.3fs\n",
			summary.LateServiceAccount.ReadyLatencySeconds,
			summary.LateNamespace.SecretLatencySeconds,
			summary.LateNamespace.ReadyLatencySeconds,
		)
		return mutationOutcome{summary: summary}
	case <-mutationCtx.Done():
		err := fmt.Errorf("injected resources did not become usable within %s: %w", timeout, mutationCtx.Err())
		summary := tracker.concurrentMutationSummary(mutation)
		summary.Error = err.Error()
		return mutationOutcome{summary: summary, err: err}
	}
}

func waitForMutationTrigger(
	ctx context.Context,
	tracker *convergenceTracker,
	phase *trackedPhase,
	triggerPercent int,
) (phaseProgress, string, string, error) {
	ticker := time.NewTicker(mutationTriggerPollInterval)
	defer ticker.Stop()
	for {
		progress, namespace, fingerprint, found := tracker.mutationTriggerCandidate(phase)
		threshold := (progress.SecretsTotal*triggerPercent + 99) / 100
		if found && progress.SecretsComplete >= threshold {
			if progress.SecretsComplete >= progress.SecretsTotal {
				return progress, "", "", errors.New("expiry refresh completed before concurrent resource injection")
			}
			return progress, namespace, fingerprint, nil
		}
		select {
		case <-ctx.Done():
			return progress, "", "", ctx.Err()
		case <-phase.done:
			return progress, "", "", errors.New("expiry refresh completed before concurrent resource injection")
		case <-ticker.C:
		}
	}
}

func createLateServiceAccount(
	ctx context.Context,
	clientset kubernetes.Interface,
	tracker *convergenceTracker,
	mutation *trackedConcurrentMutation,
	runID string,
) error {
	startedAt := time.Now()
	tracker.markLateServiceAccountRequestStarted(mutation, startedAt)
	created, err := clientset.CoreV1().ServiceAccounts(mutation.existingNamespace).Create(
		ctx,
		&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{
			Namespace: mutation.existingNamespace,
			Name:      mutation.lateServiceAccountName,
			Labels:    map[string]string{performanceRunLabel: runID},
		}},
		metav1.CreateOptions{},
	)
	if err != nil {
		return fmt.Errorf("create injected ServiceAccount %s/%s: %w", mutation.existingNamespace, mutation.lateServiceAccountName, err)
	}
	tracker.markLateServiceAccountCreated(mutation, time.Now())
	fmt.Printf("INJECT serviceaccount=%s/%s resource_version=%s\n", created.Namespace, created.Name, created.ResourceVersion)
	return nil
}

func createLateNamespace(
	ctx context.Context,
	clientset kubernetes.Interface,
	tracker *convergenceTracker,
	mutation *trackedConcurrentMutation,
	runID string,
) error {
	labels := map[string]string{performanceRunLabel: runID}
	startedAt := time.Now()
	tracker.markLateNamespaceRequestStarted(mutation, startedAt)
	created, err := clientset.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: mutation.lateNamespace, Labels: labels},
	}, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("create injected Namespace %s: %w", mutation.lateNamespace, err)
	}
	tracker.markLateNamespaceCreated(mutation, time.Now())
	fmt.Printf("INJECT namespace=%s resource_version=%s\n", created.Name, created.ResourceVersion)

	if err := ensureWriterBinding(ctx, clientset, mutation.lateNamespace, labels); err != nil {
		return err
	}
	for _, name := range targetServiceAccounts {
		// A fast controller may already have patched the auto-created default
		// account. Labelling an injected account must not undo that observation.
		if err := ensureServiceAccount(ctx, clientset, mutation.lateNamespace, name, labels, false); err != nil {
			return err
		}
	}
	tracker.markLateNamespacePrerequisitesReady(mutation, time.Now())
	return nil
}

func verifyConcurrentMutationFinalState(
	ctx context.Context,
	clientset kubernetes.Interface,
	summary concurrentMutationSummary,
) error {
	if !summary.NamespaceOnly {
		lateAccount, err := clientset.CoreV1().ServiceAccounts(summary.LateServiceAccount.Namespace).Get(
			ctx,
			summary.LateServiceAccount.Name,
			metav1.GetOptions{},
		)
		if err != nil {
			return fmt.Errorf("verify injected ServiceAccount: %w", err)
		}
		if !referencesManagedSecret(lateAccount) {
			return fmt.Errorf(
				"injected ServiceAccount %s/%s does not reference %s",
				lateAccount.Namespace,
				lateAccount.Name,
				controller.DefaultManagedSecretName,
			)
		}
	}

	secret, err := clientset.CoreV1().Secrets(summary.LateNamespace.Name).Get(
		ctx,
		controller.DefaultManagedSecretName,
		metav1.GetOptions{},
	)
	if err != nil {
		return fmt.Errorf("verify injected Namespace Secret: %w", err)
	}
	if !registrysecret.IsUsableForServiceAccount(secret) {
		return fmt.Errorf("injected Namespace Secret %s/%s is not usable", secret.Namespace, secret.Name)
	}
	if fingerprint := secretFingerprint(secret); fingerprint != summary.ExpectedCredentialFingerprint {
		return fmt.Errorf(
			"injected Namespace Secret fingerprint = %s, want current expiry generation %s",
			fingerprint,
			summary.ExpectedCredentialFingerprint,
		)
	}
	for _, name := range targetServiceAccounts {
		account, getErr := clientset.CoreV1().ServiceAccounts(summary.LateNamespace.Name).Get(
			ctx,
			name,
			metav1.GetOptions{},
		)
		if getErr != nil {
			return fmt.Errorf("verify injected Namespace ServiceAccount %s: %w", name, getErr)
		}
		if !referencesManagedSecret(account) {
			return fmt.Errorf(
				"injected Namespace ServiceAccount %s/%s does not reference %s",
				account.Namespace,
				account.Name,
				controller.DefaultManagedSecretName,
			)
		}
	}
	return nil
}

func concurrentNamespaceName(runID string) string {
	return "krsc-perf-" + runID + "-late"
}

func mutationLatency(startedAt, readyAt time.Time) float64 {
	if startedAt.IsZero() || readyAt.IsZero() || readyAt.Before(startedAt) {
		return 0
	}
	return readyAt.Sub(startedAt).Seconds()
}

func laterTime(left, right time.Time) time.Time {
	if right.After(left) {
		return right
	}
	return left
}
