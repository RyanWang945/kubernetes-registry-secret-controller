package main

import "testing"

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
