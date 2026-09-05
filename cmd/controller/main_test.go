package main

import (
	"bytes"
	"errors"
	"flag"
	"io"
	"strings"
	"testing"
)

func TestParseCommandOptionsDefaults(t *testing.T) {
	options, err := parseCommandOptions(nil, io.Discard)
	if err != nil {
		t.Fatalf("parse default options: %v", err)
	}

	want := commandOptions{
		metricsAddress:                        defaultMetricsAddress,
		healthAddress:                         defaultHealthAddress,
		leaderElection:                        true,
		kubeAPIQPS:                            defaultKubeAPIQPS,
		kubeAPIBurst:                          defaultKubeAPIBurst,
		maxConcurrentNamespaceReconciles:      2,
		maxConcurrentServiceAccountReconciles: 2,
	}
	if options != want {
		t.Fatalf("options = %#v, want %#v", options, want)
	}
}

func TestParseCommandOptionsExplicitValues(t *testing.T) {
	options, err := parseCommandOptions([]string{
		"--kubeconfig=/tmp/controller.kubeconfig",
		"--metrics-bind-address=127.0.0.1:9090",
		"--health-probe-bind-address=0",
		"--leader-elect=false",
		"--kube-api-qps=25.5",
		"--kube-api-burst=50",
		"--max-concurrent-namespace-reconciles=8",
		"--max-concurrent-service-account-reconciles=12",
	}, io.Discard)
	if err != nil {
		t.Fatalf("parse explicit options: %v", err)
	}

	want := commandOptions{
		kubeconfig:                            "/tmp/controller.kubeconfig",
		metricsAddress:                        "127.0.0.1:9090",
		healthAddress:                         "0",
		leaderElection:                        false,
		kubeAPIQPS:                            25.5,
		kubeAPIBurst:                          50,
		maxConcurrentNamespaceReconciles:      8,
		maxConcurrentServiceAccountReconciles: 12,
	}
	if options != want {
		t.Fatalf("options = %#v, want %#v", options, want)
	}
}

func TestParseCommandOptionsCanBeCalledRepeatedly(t *testing.T) {
	first, err := parseCommandOptions([]string{"--leader-elect=false"}, io.Discard)
	if err != nil {
		t.Fatalf("first parse: %v", err)
	}
	second, err := parseCommandOptions([]string{"--kubeconfig=/tmp/second"}, io.Discard)
	if err != nil {
		t.Fatalf("second parse: %v", err)
	}

	if first.leaderElection {
		t.Fatal("first leaderElection = true, want false")
	}
	if second.kubeconfig != "/tmp/second" || !second.leaderElection {
		t.Fatalf("second options = %#v", second)
	}
}

func TestParseCommandOptionsHelp(t *testing.T) {
	var output bytes.Buffer
	_, err := parseCommandOptions([]string{"--help"}, &output)
	if !errors.Is(err, flag.ErrHelp) {
		t.Fatalf("error = %v, want flag.ErrHelp", err)
	}
	if count := strings.Count(output.String(), "-kubeconfig"); count != 1 {
		t.Fatalf("help contains -kubeconfig %d times, want once:\n%s", count, output.String())
	}
}

func TestParseCommandOptionsRejectsInvalidArguments(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{
			name: "unknown flag",
			args: []string{"--not-a-controller-flag"},
			want: "flag provided but not defined",
		},
		{
			name: "positional argument",
			args: []string{"unexpected"},
			want: "unexpected positional arguments",
		},
		{
			name: "zero kube API QPS",
			args: []string{"--kube-api-qps=0"},
			want: "kube API QPS must be a finite positive value",
		},
		{
			name: "non-finite kube API QPS",
			args: []string{"--kube-api-qps=NaN"},
			want: "kube API QPS must be a finite positive value",
		},
		{
			name: "unrepresentable kube API QPS",
			args: []string{"--kube-api-qps=1e100"},
			want: "kube API QPS must be a finite positive value",
		},
		{
			name: "zero kube API burst",
			args: []string{"--kube-api-burst=0"},
			want: "kube API burst must be at least one",
		},
		{
			name: "zero namespace concurrency",
			args: []string{"--max-concurrent-namespace-reconciles=0"},
			want: "maximum concurrent namespace reconciles must be at least one",
		},
		{
			name: "zero ServiceAccount concurrency",
			args: []string{"--max-concurrent-service-account-reconciles=0"},
			want: "maximum concurrent ServiceAccount reconciles must be at least one",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := parseCommandOptions(test.args, io.Discard)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want text %q", err, test.want)
			}
		})
	}
}
