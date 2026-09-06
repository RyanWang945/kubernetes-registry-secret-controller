package observability

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
)

func TestLogLevelFiltering(t *testing.T) {
	previous := slog.Default()
	defer slog.SetDefault(previous)
	for _, level := range []string{"info", "debug"} {
		var output bytes.Buffer
		logger, err := ConfigureLogging(&output, level)
		if err != nil {
			t.Fatal(err)
		}
		logger.V(1).Info("dependency wait")
		logger.Info("credential refreshed", "registry", "cn-test/cri-a")
		if strings.Contains(output.String(), "dependency wait") != (level == "debug") {
			t.Fatalf("incorrect %s log filtering", level)
		}
		if !strings.Contains(output.String(), `"registry":"cn-test/cri-a"`) {
			t.Fatal("missing JSON context")
		}
	}
	if _, err := LogLevel("invalid"); err == nil {
		t.Fatal("invalid level accepted")
	}
}

func TestSecureMetricsRejectsMissingCertificate(t *testing.T) {
	if _, err := ServerOptions(":8443", true, ""); err == nil {
		t.Fatal("secure metrics accepted missing certificate directory")
	}
	if _, err := ServerOptions(":8443", true, t.TempDir()); err == nil {
		t.Fatal("secure metrics silently fell back to a self-signed certificate")
	}
	plain, err := ServerOptions(":8080", false, "")
	if err != nil || plain.SecureServing || plain.FilterProvider != nil {
		t.Fatal("plain metrics configuration changed")
	}
}
