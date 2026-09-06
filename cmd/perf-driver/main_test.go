package main

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestInjectedAccountPreparationPreservesCompletedPatch(t *testing.T) {
	for _, reset := range []bool{false, true} {
		account := testServiceAccount("ns-late", "default", "1")
		account.ImagePullSecrets = append(account.ImagePullSecrets, corev1.LocalObjectReference{Name: "user-secret"})
		clientset := fake.NewSimpleClientset(account)
		labels := map[string]string{performanceRunLabel: "blog"}
		if err := ensureServiceAccount(context.Background(), clientset, "ns-late", "default", labels, reset); err != nil {
			t.Fatal(err)
		}
		got, err := clientset.CoreV1().ServiceAccounts("ns-late").Get(context.Background(), "default", metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if referencesManagedSecret(got) == reset || got.Labels[performanceRunLabel] != "blog" {
			t.Fatalf("reset=%t, unexpected account: %+v", reset, got)
		}
		if got.ImagePullSecrets[len(got.ImagePullSecrets)-1].Name != "user-secret" {
			t.Fatal("user reference was removed")
		}
	}
}

func TestNamespaceOnlyRequiresExpiryInjection(t *testing.T) {
	args := []string{"--run-id=ns-only", "--namespaces=50", "--inject-namespace-only"}
	if _, err := parseOptions(args); err == nil {
		t.Fatal("namespace-only injection without expiry was accepted")
	}
	options, err := parseOptions(append(args, "--inject-during-expiry"))
	if err != nil || !options.injectNamespaceOnly {
		t.Fatalf("namespace-only options = %+v, err=%v", options, err)
	}
}

func TestSummarizeControllerLogsDetectsCompleteness(t *testing.T) {
	complete := []byte(
		`{"level":"INFO","msg":"starting E2E controller manager with mock provider"}` + "\n" +
			`{"level":"ERROR","msg":"first"}` + "\n" +
			`{"level":"error","msg":"second"}` + "\n",
	)
	diagnostics := summarizeControllerLogs(complete)
	if !diagnostics.ControllerLogComplete {
		t.Fatal("complete log was reported incomplete")
	}
	if diagnostics.ControllerLogWarning != "" {
		t.Fatalf("complete log warning = %q", diagnostics.ControllerLogWarning)
	}
	if diagnostics.ControllerErrorLogLines != 2 {
		t.Fatalf("error lines = %d, want 2", diagnostics.ControllerErrorLogLines)
	}

	truncated := summarizeControllerLogs([]byte(`{"level":"ERROR","msg":"tail only"}`))
	if truncated.ControllerLogComplete {
		t.Fatal("truncated log was reported complete")
	}
	if truncated.ControllerLogWarning == "" {
		t.Fatal("truncated log warning is empty")
	}
}

func TestParseOptionsConcurrentMutation(t *testing.T) {
	options, err := parseOptions([]string{
		"--run-id=mutation",
		"--namespaces=1000",
		"--inject-during-expiry",
		"--injection-percent=30",
		"--injection-timeout=2m",
		"--environment-note=concurrent-namespace-syncs=30",
	})
	if err != nil {
		t.Fatalf("parseOptions() error = %v", err)
	}
	if !options.injectDuringExpiry || options.injectionPercent != 30 || options.injectionTimeout.String() != "2m0s" {
		t.Fatalf("unexpected mutation options: %+v", options)
	}
	if options.environmentNote != "concurrent-namespace-syncs=30" {
		t.Fatalf("environment note = %q", options.environmentNote)
	}

	for _, percent := range []string{"0", "100"} {
		if _, err := parseOptions([]string{
			"--run-id=mutation",
			"--namespaces=1",
			"--injection-percent=" + percent,
		}); err == nil {
			t.Fatalf("injection percent %s was accepted", percent)
		}
	}
}
