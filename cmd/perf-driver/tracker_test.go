package main

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/RyanWang945/kubernetes-registry-secret-controller/internal/controller"
	"github.com/RyanWang945/kubernetes-registry-secret-controller/internal/registrysecret"
)

func TestTrackerRecordsInitialAndExpiryConvergence(t *testing.T) {
	tracker := newConvergenceTracker([]string{"ns-a"}, []string{"default", "workload"})
	start := time.Date(2026, time.September, 4, 10, 0, 0, 0, time.UTC)
	initial := tracker.start(initialPhase, start)
	tracker.observeSecret(testSecret("ns-a", "one"), start.Add(time.Second))
	tracker.observeServiceAccount(testServiceAccount("ns-a", "default", "1"), start.Add(2*time.Second))
	tracker.observeServiceAccount(testServiceAccount("ns-a", "workload", "2"), start.Add(3*time.Second))

	select {
	case <-initial.done:
	default:
		t.Fatal("initial phase did not complete")
	}
	if summary := tracker.summary(initial); summary.DurationSeconds != 3 || summary.SecretCompletion.Count != 1 || summary.ServiceAccountCompletion.Count != 2 {
		t.Fatalf("unexpected initial summary: %+v", summary)
	} else if summary.SecretCompletion.ObjectsPerSecond != 1 || summary.ServiceAccountCompletion.ObjectsPerSecond != 2.0/3.0 {
		t.Fatalf("throughput must use each resource's own completion time: %+v", summary)
	}

	expiryStart := start.Add(10 * time.Second)
	expiry := tracker.start(expiryPhase, expiryStart)
	tracker.observeSecret(testSecret("ns-a", "one"), expiryStart.Add(time.Second))
	tracker.observeServiceAccount(testServiceAccount("ns-a", "default", "3"), expiryStart.Add(time.Second))
	tracker.observeSecret(testSecret("ns-a", "two"), expiryStart.Add(2*time.Second))

	select {
	case <-expiry.done:
	default:
		t.Fatal("expiry phase did not complete")
	}
	summary := tracker.summary(expiry)
	if summary.DurationSeconds != 2 || summary.UnexpectedServiceAccounts != 1 {
		t.Fatalf("unexpected expiry summary: %+v", summary)
	}
}

func TestPercentileInterpolates(t *testing.T) {
	values := []float64{1, 2, 3, 4, 5}
	if got := percentile(values, 0.90); got != 4.6 {
		t.Fatalf("p90 = %v, want 4.6", got)
	}
}

func TestTrackerRecordsConcurrentMutationReadiness(t *testing.T) {
	tracker := newConvergenceTracker([]string{"ns-a", "ns-b"}, []string{"default", "workload"})
	start := time.Date(2026, time.September, 4, 10, 0, 0, 0, time.UTC)
	initial := tracker.start(initialPhase, start)
	for _, namespace := range []string{"ns-a", "ns-b"} {
		tracker.observeSecret(testSecret(namespace, "old"), start.Add(time.Second))
		tracker.observeServiceAccount(testServiceAccount(namespace, "default", "1"), start.Add(time.Second))
		tracker.observeServiceAccount(testServiceAccount(namespace, "workload", "2"), start.Add(time.Second))
	}
	select {
	case <-initial.done:
	default:
		t.Fatal("initial phase did not complete")
	}

	expiryStarted := start.Add(10 * time.Second)
	expiry := tracker.start(expiryPhase, expiryStarted)
	updatedSecret := testSecret("ns-a", "new")
	tracker.observeSecret(updatedSecret, expiryStarted.Add(time.Second))
	progress, namespace, fingerprint, found := tracker.mutationTriggerCandidate(expiry)
	if !found || namespace != "ns-a" || fingerprint != secretFingerprint(updatedSecret) {
		t.Fatalf("trigger candidate = (%+v, %q, %q, %t)", progress, namespace, fingerprint, found)
	}

	triggeredAt := expiryStarted.Add(2 * time.Second)
	mutation := tracker.startConcurrentMutation(
		25,
		triggeredAt,
		progress,
		namespace,
		serviceAccountLate,
		"ns-late",
		targetServiceAccounts,
		fingerprint,
	)
	tracker.markLateServiceAccountRequestStarted(mutation, triggeredAt)
	tracker.markLateServiceAccountCreated(mutation, triggeredAt.Add(100*time.Millisecond))
	tracker.markLateNamespaceRequestStarted(mutation, triggeredAt)
	tracker.markLateNamespaceCreated(mutation, triggeredAt.Add(200*time.Millisecond))
	tracker.markLateNamespacePrerequisitesReady(mutation, triggeredAt.Add(300*time.Millisecond))

	tracker.observeSecret(testSecret("ns-late", "old"), triggeredAt.Add(400*time.Millisecond))
	tracker.observeServiceAccount(testServiceAccount("ns-a", serviceAccountLate, "3"), triggeredAt.Add(500*time.Millisecond))
	tracker.observeServiceAccount(testServiceAccount("ns-late", "default", "4"), triggeredAt.Add(600*time.Millisecond))
	tracker.observeServiceAccount(testServiceAccount("ns-late", "workload", "5"), triggeredAt.Add(700*time.Millisecond))
	select {
	case <-mutation.done:
		t.Fatal("mutation completed with a stale injected Namespace Secret")
	default:
	}

	tracker.observeSecret(testSecret("ns-late", "new"), triggeredAt.Add(800*time.Millisecond))
	select {
	case <-mutation.done:
	default:
		t.Fatal("mutation did not complete")
	}

	summary := tracker.concurrentMutationSummary(mutation)
	if summary.SecretsCompleteAtTrigger != 1 || summary.SecretsTotalAtTrigger != 2 {
		t.Fatalf("trigger progress = %d/%d, want 1/2", summary.SecretsCompleteAtTrigger, summary.SecretsTotalAtTrigger)
	}
	if summary.LateServiceAccount.ReadyLatencySeconds != 0.4 {
		t.Fatalf("late ServiceAccount latency = %v, want 0.4", summary.LateServiceAccount.ReadyLatencySeconds)
	}
	if summary.LateNamespace.ReadyLatencySeconds != 0.6 {
		t.Fatalf("late Namespace readiness latency = %v, want 0.6", summary.LateNamespace.ReadyLatencySeconds)
	}
	if summary.LateNamespace.ObservedCredentialFingerprint != fingerprint {
		t.Fatalf("late Namespace fingerprint = %q, want %q", summary.LateNamespace.ObservedCredentialFingerprint, fingerprint)
	}
	if summary.LateNamespace.StaleCredentialFingerprints != 1 {
		t.Fatalf("stale fingerprint count = %d, want 1", summary.LateNamespace.StaleCredentialFingerprints)
	}
}

func testSecret(namespace, value string) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      controller.DefaultManagedSecretName,
			Namespace: namespace,
			Labels: map[string]string{
				registrysecret.ApplicationNameLabelKey: registrysecret.ControllerIdentity,
				registrysecret.ManagedByLabelKey:       registrysecret.ControllerIdentity,
			},
			Annotations: map[string]string{registrysecret.StateAnnotationKey: value},
		},
		Type: corev1.SecretTypeDockerConfigJson,
		Data: map[string][]byte{corev1.DockerConfigJsonKey: []byte(value)},
	}
}

func testServiceAccount(namespace, name, resourceVersion string) *corev1.ServiceAccount {
	return &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			Namespace:       namespace,
			ResourceVersion: resourceVersion,
		},
		ImagePullSecrets: []corev1.LocalObjectReference{{Name: controller.DefaultManagedSecretName}},
	}
}
