// Command perf-driver creates an isolated Kubernetes workload, measures the
// controller's initial Secret/ServiceAccount convergence, advances the E2E
// controller's fake clock, and measures the real expiry-refresh fan-out.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/retry"
	"k8s.io/utils/ptr"

	"github.com/RyanWang945/kubernetes-registry-secret-controller/internal/config"
	"github.com/RyanWang945/kubernetes-registry-secret-controller/internal/controller"
	"github.com/RyanWang945/kubernetes-registry-secret-controller/internal/registrysecret"
)

const (
	performanceRunLabel      = "registry-secret-controller.io/performance-run"
	controllerNamespace      = controller.DefaultControllerNamespace
	controllerDeployment     = "registry-secret-controller-perf"
	controllerApplication    = "registry-secret-controller-perf"
	clockControlService      = "registry-secret-controller-perf-clock"
	clockControlServicePort  = "clock"
	serviceAccountDefault    = "default"
	serviceAccountWorkload   = "workload"
	serviceAccountLate       = "late-workload"
	defaultSetupConcurrency  = 25
	defaultClientQPS         = 100
	defaultClientBurst       = 200
	defaultInitialTimeout    = 3 * time.Hour
	defaultExpiryTimeout     = 90 * time.Minute
	defaultCleanupTimeout    = 30 * time.Minute
	defaultClockAdvance      = 50 * time.Minute
	observerSyncTimeout      = 10 * time.Minute
	postInitialSettleTime    = 2 * time.Second
	progressSampleInterval   = time.Second
	progressLogInterval      = 10 * time.Second
	defaultInjectionPercent  = 25
	defaultInjectionTimeout  = 10 * time.Minute
	maximumConfigMapDataSize = 900 * 1024
)

var targetServiceAccounts = []string{serviceAccountDefault, serviceAccountWorkload}

type commandOptions struct {
	kubeconfig          string
	contextName         string
	runID               string
	datasetID           string
	outputDirectory     string
	namespaceCount      int
	setupConcurrency    int
	clientQPS           float64
	clientBurst         int
	initialTimeout      time.Duration
	expiryTimeout       time.Duration
	cleanupTimeout      time.Duration
	clockAdvance        time.Duration
	cleanupOnly         bool
	keepResources       bool
	reuseDataset        bool
	retainDataset       bool
	injectDuringExpiry  bool
	injectNamespaceOnly bool
	injectionPercent    int
	injectionTimeout    time.Duration
	environmentNote     string
}

type benchmarkSummary struct {
	SchemaVersion       int                        `json:"schemaVersion"`
	RunID               string                     `json:"runID"`
	DatasetID           string                     `json:"datasetID"`
	NamespaceCount      int                        `json:"namespaceCount"`
	ServiceAccountCount int                        `json:"serviceAccountCount"`
	StartedAt           time.Time                  `json:"startedAt"`
	FinishedAt          time.Time                  `json:"finishedAt"`
	SetupSeconds        float64                    `json:"setupSeconds"`
	CleanupSeconds      float64                    `json:"cleanupSeconds"`
	Initial             *phaseSummary              `json:"initial,omitempty"`
	Expiry              *phaseSummary              `json:"expiry,omitempty"`
	Resources           resourceSummary            `json:"resources"`
	Diagnostics         diagnosticsSummary         `json:"diagnostics"`
	ControllerProfile   controllerProfile          `json:"controllerProfile"`
	ConcurrentMutation  *concurrentMutationSummary `json:"concurrentMutation,omitempty"`
	EnvironmentNote     string                     `json:"environmentNote,omitempty"`
	Success             bool                       `json:"success"`
	Error               string                     `json:"error,omitempty"`
}

type diagnosticsSummary struct {
	ControllerLogBytes      int    `json:"controllerLogBytes"`
	ControllerErrorLogLines int    `json:"controllerErrorLogLines"`
	ControllerLogComplete   bool   `json:"controllerLogComplete"`
	ControllerLogWarning    string `json:"controllerLogWarning,omitempty"`
	CollectionError         string `json:"collectionError,omitempty"`
}

type controllerProfile struct {
	Replicas                           int       `json:"replicas"`
	Image                              string    `json:"image"`
	Arguments                          []string  `json:"arguments"`
	DeploymentGeneration               int64     `json:"deploymentGeneration"`
	ObservedAt                         time.Time `json:"observedAt"`
	VerifiedAt                         time.Time `json:"verifiedAt"`
	ConfigurationStable                bool      `json:"configurationStable"`
	NamespaceReconcileConcurrency      int       `json:"namespaceReconcileConcurrency"`
	ServiceAccountReconcileConcurrency int       `json:"serviceAccountReconcileConcurrency"`
	RESTClientRateLimit                string    `json:"restClientRateLimit"`
	TokenTTL                           string    `json:"tokenTTL"`
	RefreshBefore                      string    `json:"refreshBefore"`
	ExpiryTrigger                      string    `json:"expiryTrigger"`
}

func main() {
	options, err := parseOptions(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	clientset, err := newClientset(options)
	if err != nil {
		fmt.Fprintf(os.Stderr, "create Kubernetes client: %v\n", err)
		os.Exit(1)
	}

	if options.cleanupOnly {
		if err := cleanupOnly(ctx, clientset, options); err != nil {
			fmt.Fprintf(os.Stderr, "cleanup: %v\n", err)
			os.Exit(1)
		}
		return
	}

	if err := os.MkdirAll(options.outputDirectory, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "create output directory: %v\n", err)
		os.Exit(1)
	}
	summary, runErr := runBenchmark(ctx, clientset, options)
	summary.FinishedAt = time.Now().UTC()
	if runErr != nil {
		summary.Error = runErr.Error()
	}
	if err := writeSummary(options, summary); err != nil {
		fmt.Fprintf(os.Stderr, "write summary: %v\n", err)
		if runErr == nil {
			runErr = err
		}
	}
	if runErr != nil {
		fmt.Fprintf(os.Stderr, "benchmark failed: %v\n", runErr)
		os.Exit(1)
	}
}

func parseOptions(arguments []string) (commandOptions, error) {
	options := commandOptions{}
	flags := flag.NewFlagSet("perf-driver", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	flags.StringVar(&options.kubeconfig, "kubeconfig", "", "path to kubeconfig; empty uses standard loading rules")
	flags.StringVar(&options.contextName, "context", "", "kubeconfig context override")
	flags.StringVar(&options.runID, "run-id", "", "DNS-label identifier used to isolate one benchmark run")
	flags.StringVar(&options.datasetID, "dataset-id", "", "DNS-label identifier for a reusable Namespace set; defaults to run-id")
	flags.StringVar(&options.outputDirectory, "output-dir", "test/performance/results", "directory for JSON and progress CSV files")
	flags.IntVar(&options.namespaceCount, "namespaces", 0, "number of test namespaces")
	flags.IntVar(&options.setupConcurrency, "setup-concurrency", defaultSetupConcurrency, "maximum concurrent setup and cleanup calls")
	flags.Float64Var(&options.clientQPS, "client-qps", defaultClientQPS, "performance driver's Kubernetes client QPS")
	flags.IntVar(&options.clientBurst, "client-burst", defaultClientBurst, "performance driver's Kubernetes client burst")
	flags.DurationVar(&options.initialTimeout, "initial-timeout", defaultInitialTimeout, "timeout for initial Secret and ServiceAccount convergence")
	flags.DurationVar(&options.expiryTimeout, "expiry-timeout", defaultExpiryTimeout, "timeout for the expiry refresh fan-out")
	flags.DurationVar(&options.cleanupTimeout, "cleanup-timeout", defaultCleanupTimeout, "timeout for exact-run cleanup")
	flags.DurationVar(&options.clockAdvance, "clock-advance", defaultClockAdvance, "fake-clock step used to reach the credential refresh time")
	flags.BoolVar(&options.cleanupOnly, "cleanup-only", false, "delete only resources labeled with run-id, then exit")
	flags.BoolVar(&options.keepResources, "keep-resources", false, "leave test namespaces and the controller running after the benchmark")
	flags.BoolVar(&options.reuseDataset, "reuse-dataset", false, "reuse and reset an existing dataset, growing it to namespaces if needed")
	flags.BoolVar(&options.retainDataset, "retain-dataset", false, "scale the controller to zero but retain test namespaces after the benchmark")
	flags.BoolVar(&options.injectDuringExpiry, "inject-during-expiry", false, "create a target ServiceAccount and Namespace while expiry refresh is in progress")
	flags.BoolVar(&options.injectNamespaceOnly, "inject-namespace-only", false, "inject only a Namespace with two ServiceAccounts, keeping every Namespace at two accounts; requires --inject-during-expiry")
	flags.IntVar(&options.injectionPercent, "injection-percent", defaultInjectionPercent, "percentage of existing Secrets refreshed before injecting new resources")
	flags.DurationVar(&options.injectionTimeout, "injection-timeout", defaultInjectionTimeout, "timeout for injected resources to become usable")
	flags.StringVar(&options.environmentNote, "environment-note", "", "free-form cluster configuration note stored with the result")
	if err := flags.Parse(arguments); err != nil {
		return commandOptions{}, err
	}
	if flags.NArg() != 0 {
		return commandOptions{}, fmt.Errorf("unexpected positional arguments: %s", strings.Join(flags.Args(), " "))
	}
	if options.runID == "" {
		return commandOptions{}, errors.New("--run-id is required")
	}
	if options.injectNamespaceOnly && !options.injectDuringExpiry {
		return commandOptions{}, errors.New("--inject-namespace-only requires --inject-during-expiry")
	}
	if problems := validation.IsDNS1123Label(options.runID); len(problems) > 0 {
		return commandOptions{}, fmt.Errorf("invalid --run-id %q: %s", options.runID, strings.Join(problems, "; "))
	}
	if options.datasetID == "" {
		options.datasetID = options.runID
	}
	if problems := validation.IsDNS1123Label(options.datasetID); len(problems) > 0 {
		return commandOptions{}, fmt.Errorf("invalid --dataset-id %q: %s", options.datasetID, strings.Join(problems, "; "))
	}
	if !options.cleanupOnly && options.namespaceCount <= 0 {
		return commandOptions{}, errors.New("--namespaces must be greater than zero")
	}
	if !options.cleanupOnly {
		generatedNames := namespaceNames(options.datasetID, options.namespaceCount)
		for _, name := range []string{generatedNames[0], generatedNames[len(generatedNames)-1]} {
			if problems := validation.IsDNS1123Label(name); len(problems) > 0 {
				return commandOptions{}, fmt.Errorf("dataset ID produces invalid Namespace name %q: %s", name, strings.Join(problems, "; "))
			}
		}
		if options.injectDuringExpiry {
			name := concurrentNamespaceName(options.datasetID)
			if problems := validation.IsDNS1123Label(name); len(problems) > 0 {
				return commandOptions{}, fmt.Errorf("dataset ID produces invalid injected Namespace name %q: %s", name, strings.Join(problems, "; "))
			}
		}
	}
	if options.setupConcurrency <= 0 || options.clientQPS <= 0 || options.clientBurst <= 0 {
		return commandOptions{}, errors.New("setup concurrency, client QPS, and client burst must be positive")
	}
	if options.initialTimeout <= 0 || options.expiryTimeout <= 0 || options.cleanupTimeout <= 0 || options.clockAdvance <= 0 {
		return commandOptions{}, errors.New("timeouts and clock advance must be positive")
	}
	if options.injectionTimeout <= 0 {
		return commandOptions{}, errors.New("injection timeout must be positive")
	}
	if options.injectionPercent < 1 || options.injectionPercent > 99 {
		return commandOptions{}, errors.New("injection percent must be between 1 and 99")
	}
	return options, nil
}

func newClientset(options commandOptions) (kubernetes.Interface, error) {
	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	if options.kubeconfig != "" {
		loadingRules.ExplicitPath = options.kubeconfig
	}
	overrides := &clientcmd.ConfigOverrides{CurrentContext: options.contextName}
	restConfig, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, overrides).ClientConfig()
	if err != nil {
		return nil, err
	}
	restConfig.QPS = float32(options.clientQPS)
	restConfig.Burst = options.clientBurst
	restConfig.UserAgent = "kubernetes-registry-secret-controller-perf-driver"
	return kubernetes.NewForConfig(rest.CopyConfig(restConfig))
}

func runBenchmark(
	ctx context.Context,
	clientset kubernetes.Interface,
	options commandOptions,
) (summary benchmarkSummary, returnErr error) {
	summary = benchmarkSummary{
		SchemaVersion:       2,
		RunID:               options.runID,
		DatasetID:           options.datasetID,
		NamespaceCount:      options.namespaceCount,
		ServiceAccountCount: options.namespaceCount * len(targetServiceAccounts),
		StartedAt:           time.Now().UTC(),
		EnvironmentNote:     options.environmentNote,
	}
	profile, err := readControllerProfile(ctx, clientset)
	if err != nil {
		return summary, err
	}
	profile.ExpiryTrigger = fmt.Sprintf(
		"fake clock advanced %s; real scheduler and Kubernetes writes",
		options.clockAdvance,
	)
	summary.ControllerProfile = profile

	if err := setDeploymentReplicas(ctx, clientset, 0); err != nil {
		return summary, err
	}
	if err := waitForControllerPods(ctx, clientset, false, 5*time.Minute); err != nil {
		return summary, err
	}
	if !options.reuseDataset {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), options.cleanupTimeout)
		if err := cleanupRun(cleanupCtx, clientset, options.datasetID, options.setupConcurrency); err != nil {
			cleanupCancel()
			return summary, fmt.Errorf("clean stale run resources: %w", err)
		}
		cleanupCancel()
	} else if err := validateReusableDataset(ctx, clientset, options.datasetID, options.namespaceCount); err != nil {
		return summary, err
	}
	if !options.keepResources {
		defer func() {
			cleanupStarted := time.Now()
			cleanupCtx, cancel := context.WithTimeout(context.Background(), options.cleanupTimeout)
			defer cancel()
			logPath := filepath.Join(options.outputDirectory, options.runID+"-controller.log")
			diagnostics, diagnosticsErr := collectControllerLogs(cleanupCtx, clientset, logPath)
			if diagnosticsErr != nil {
				diagnostics.CollectionError = diagnosticsErr.Error()
			}
			summary.Diagnostics = diagnostics
			cleanupErr := setDeploymentReplicas(cleanupCtx, clientset, 0)
			if cleanupErr == nil {
				cleanupErr = waitForControllerPods(cleanupCtx, clientset, false, 5*time.Minute)
			}
			if cleanupErr == nil && !options.retainDataset {
				cleanupErr = cleanupRun(cleanupCtx, clientset, options.datasetID, options.setupConcurrency)
			}
			summary.CleanupSeconds = time.Since(cleanupStarted).Seconds()
			if cleanupErr != nil {
				summary.Success = false
				cleanupErr = fmt.Errorf("cleanup benchmark resources: %w", cleanupErr)
				if returnErr == nil {
					returnErr = cleanupErr
				} else {
					returnErr = errors.Join(returnErr, cleanupErr)
				}
			}
		}()
	}

	namespaces := namespaceNames(options.datasetID, options.namespaceCount)
	configuredNamespaces := append([]string(nil), namespaces...)
	configuredServiceAccounts := append([]string(nil), targetServiceAccounts...)
	if options.injectDuringExpiry {
		configuredNamespaces = append(configuredNamespaces, concurrentNamespaceName(options.datasetID))
		if !options.injectNamespaceOnly {
			configuredServiceAccounts = append(configuredServiceAccounts, serviceAccountLate)
		}
	}
	setupStarted := time.Now()
	if err := prepareWorkload(ctx, clientset, options.datasetID, namespaces, options.setupConcurrency); err != nil {
		return summary, fmt.Errorf("prepare workload: %w", err)
	}
	if err := resetManagedSecrets(ctx, clientset, namespaces, options.setupConcurrency); err != nil {
		return summary, fmt.Errorf("reset managed Secrets: %w", err)
	}
	if err := configureTargets(ctx, clientset, configuredNamespaces, configuredServiceAccounts); err != nil {
		return summary, fmt.Errorf("configure targets: %w", err)
	}
	tracker := newConvergenceTracker(namespaces, targetServiceAccounts)
	observerCtx, stopObservers := context.WithTimeout(ctx, observerSyncTimeout+options.initialTimeout+options.expiryTimeout)
	defer stopObservers()
	selector := performanceRunLabel + "=" + options.datasetID
	if _, err := startConvergenceObserver(observerCtx, clientset, selector, tracker); err != nil {
		return summary, err
	}
	resourcePath := filepath.Join(options.outputDirectory, options.runID+"-resources.csv")
	resources, err := newResourceRecorder(resourcePath)
	if err != nil {
		return summary, err
	}
	defer func() {
		summary.Resources = resources.Summary()
		_ = resources.Close()
	}()
	summary.SetupSeconds = time.Since(setupStarted).Seconds()

	initialStarted := time.Now()
	initial := tracker.start(initialPhase, initialStarted)
	if err := setDeploymentReplicas(ctx, clientset, 1); err != nil {
		return summary, fmt.Errorf("start performance controller: %w", err)
	}
	initialProgress := filepath.Join(options.outputDirectory, options.runID+"-initial-progress.csv")
	if err := waitForPhase(ctx, clientset, tracker, initial, resources, initialProgress, options.initialTimeout); err != nil {
		partial := tracker.summary(initial)
		summary.Initial = &partial
		return summary, err
	}
	initialSummary := tracker.summary(initial)
	summary.Initial = &initialSummary
	if err := waitForControllerPods(ctx, clientset, true, 5*time.Minute); err != nil {
		return summary, err
	}

	select {
	case <-ctx.Done():
		return summary, ctx.Err()
	case <-time.After(postInitialSettleTime):
	}

	expiryStarted := time.Now()
	expiry := tracker.start(expiryPhase, expiryStarted)
	var (
		mutationCancel  context.CancelFunc
		mutationResults chan mutationOutcome
	)
	if options.injectDuringExpiry {
		mutationCtx, cancel := context.WithCancel(ctx)
		mutationCancel = cancel
		mutationResults = make(chan mutationOutcome, 1)
		go func() {
			mutationResults <- runConcurrentMutation(
				mutationCtx,
				clientset,
				tracker,
				expiry,
				options.datasetID,
				options.injectionPercent,
				options.injectionTimeout,
				options.injectNamespaceOnly,
			)
		}()
	}
	if err := advanceControllerClock(ctx, clientset, options.clockAdvance); err != nil {
		if mutationCancel != nil {
			mutationCancel()
			outcome := <-mutationResults
			summary.ConcurrentMutation = &outcome.summary
		}
		return summary, err
	}
	expiryProgress := filepath.Join(options.outputDirectory, options.runID+"-expiry-progress.csv")
	if err := waitForPhase(ctx, clientset, tracker, expiry, resources, expiryProgress, options.expiryTimeout); err != nil {
		partial := tracker.summary(expiry)
		summary.Expiry = &partial
		if mutationCancel != nil {
			mutationCancel()
			outcome := <-mutationResults
			summary.ConcurrentMutation = &outcome.summary
		}
		return summary, err
	}
	expirySummary := tracker.summary(expiry)
	summary.Expiry = &expirySummary
	if mutationCancel != nil {
		outcome := <-mutationResults
		mutationCancel()
		summary.ConcurrentMutation = &outcome.summary
		if outcome.err != nil {
			return summary, outcome.err
		}
	}
	verifiedProfile, err := readControllerProfile(ctx, clientset)
	if err != nil {
		return summary, err
	}
	if profile.Image != verifiedProfile.Image || !reflect.DeepEqual(profile.Arguments, verifiedProfile.Arguments) {
		return summary, fmt.Errorf("performance controller image or arguments changed during benchmark")
	}
	summary.ControllerProfile.DeploymentGeneration = verifiedProfile.DeploymentGeneration
	summary.ControllerProfile.VerifiedAt = verifiedProfile.ObservedAt
	summary.ControllerProfile.ConfigurationStable = true
	if err := verifyFinalState(ctx, clientset, namespaces, selector); err != nil {
		return summary, err
	}
	if options.injectDuringExpiry {
		if err := verifyConcurrentMutationFinalState(ctx, clientset, *summary.ConcurrentMutation); err != nil {
			return summary, err
		}
	}
	summary.Success = true
	return summary, nil
}

func collectControllerLogs(
	ctx context.Context,
	clientset kubernetes.Interface,
	path string,
) (diagnosticsSummary, error) {
	pods, err := clientset.CoreV1().Pods(controllerNamespace).List(ctx, metav1.ListOptions{
		LabelSelector: "app.kubernetes.io/name=" + controllerApplication,
	})
	if err != nil {
		return diagnosticsSummary{}, err
	}
	var combined bytes.Buffer
	for index := range pods.Items {
		pod := &pods.Items[index]
		raw, logErr := clientset.CoreV1().Pods(controllerNamespace).GetLogs(
			pod.Name,
			&corev1.PodLogOptions{Container: "controller"},
		).DoRaw(ctx)
		if logErr != nil {
			return diagnosticsSummary{}, logErr
		}
		fmt.Fprintf(&combined, "# pod=%s node=%s\n", pod.Name, pod.Spec.NodeName)
		combined.Write(raw)
		if len(raw) > 0 && raw[len(raw)-1] != '\n' {
			combined.WriteByte('\n')
		}
	}
	data := combined.Bytes()
	diagnostics := summarizeControllerLogs(data)
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return diagnostics, err
	}
	return diagnostics, nil
}

func summarizeControllerLogs(data []byte) diagnosticsSummary {
	diagnostics := diagnosticsSummary{
		ControllerLogBytes: len(data),
		ControllerErrorLogLines: bytes.Count(data, []byte(`"level":"error"`)) +
			bytes.Count(data, []byte(`"level":"ERROR"`)),
		ControllerLogComplete: bytes.Contains(
			data,
			[]byte(`"msg":"starting E2E controller manager with mock provider"`),
		),
	}
	if len(data) > 0 && !diagnostics.ControllerLogComplete {
		diagnostics.ControllerLogWarning = "startup marker absent; Kubernetes container log rotation may have truncated earlier lines"
	}
	return diagnostics
}

func cleanupOnly(ctx context.Context, clientset kubernetes.Interface, options commandOptions) error {
	if err := setDeploymentReplicas(ctx, clientset, 0); err != nil {
		return err
	}
	if err := waitForControllerPods(ctx, clientset, false, 5*time.Minute); err != nil {
		return err
	}
	cleanupCtx, cancel := context.WithTimeout(ctx, options.cleanupTimeout)
	defer cancel()
	return cleanupRun(cleanupCtx, clientset, options.datasetID, options.setupConcurrency)
}

func namespaceNames(runID string, count int) []string {
	names := make([]string, count)
	for index := range count {
		names[index] = fmt.Sprintf("krsc-perf-%s-%05d", runID, index)
	}
	return names
}

func validateReusableDataset(
	ctx context.Context,
	clientset kubernetes.Interface,
	datasetID string,
	desiredCount int,
) error {
	expected := make(map[string]struct{}, desiredCount)
	for _, namespace := range namespaceNames(datasetID, desiredCount) {
		expected[namespace] = struct{}{}
	}
	existing, err := clientset.CoreV1().Namespaces().List(ctx, metav1.ListOptions{
		LabelSelector: performanceRunLabel + "=" + datasetID,
	})
	if err != nil {
		return fmt.Errorf("list reusable dataset: %w", err)
	}
	for index := range existing.Items {
		if _, wanted := expected[existing.Items[index].Name]; !wanted {
			return fmt.Errorf(
				"reusable dataset %s contains Namespace %s outside the requested first %d entries",
				datasetID,
				existing.Items[index].Name,
				desiredCount,
			)
		}
	}
	return nil
}

func prepareWorkload(
	ctx context.Context,
	clientset kubernetes.Interface,
	runID string,
	namespaces []string,
	concurrency int,
) error {
	var completed atomic.Int64
	logEvery := int64(max(1, len(namespaces)/10))
	err := runParallel(ctx, namespaces, concurrency, func(ctx context.Context, namespace string) error {
		labels := map[string]string{performanceRunLabel: runID}
		created, err := clientset.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: namespace, Labels: labels},
		}, metav1.CreateOptions{})
		if apierrors.IsAlreadyExists(err) {
			created, err = clientset.CoreV1().Namespaces().Get(ctx, namespace, metav1.GetOptions{})
			if err == nil && created.Labels[performanceRunLabel] != runID {
				return fmt.Errorf("Namespace %s exists without expected run label", namespace)
			}
		}
		if err != nil {
			return fmt.Errorf("create Namespace %s: %w", namespace, err)
		}
		for _, name := range targetServiceAccounts {
			if err := ensureServiceAccount(ctx, clientset, namespace, name, labels, true); err != nil {
				return err
			}
		}
		if err := ensureWriterBinding(ctx, clientset, namespace, labels); err != nil {
			return err
		}
		current := completed.Add(1)
		if current%logEvery == 0 || current == int64(len(namespaces)) {
			fmt.Printf("SETUP namespaces=%d/%d\n", current, len(namespaces))
		}
		return nil
	})
	return err
}

func ensureWriterBinding(
	ctx context.Context,
	clientset kubernetes.Interface,
	namespace string,
	labels map[string]string,
) error {
	const bindingName = "registry-secret-controller-perf-writer"
	desired := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: bindingName, Namespace: namespace, Labels: labels},
		RoleRef: rbacv1.RoleRef{
			APIGroup: rbacv1.GroupName,
			Kind:     "ClusterRole",
			Name:     "registry-secret-controller-perf-writer",
		},
		Subjects: []rbacv1.Subject{{
			Kind:      rbacv1.ServiceAccountKind,
			Name:      "registry-secret-controller",
			Namespace: controllerNamespace,
		}},
	}
	_, err := clientset.RbacV1().RoleBindings(namespace).Create(ctx, desired, metav1.CreateOptions{})
	if err == nil {
		return nil
	}
	if !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("create writer RoleBinding in %s: %w", namespace, err)
	}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		current, getErr := clientset.RbacV1().RoleBindings(namespace).Get(ctx, bindingName, metav1.GetOptions{})
		if getErr != nil {
			return getErr
		}
		if current.Labels[performanceRunLabel] == labels[performanceRunLabel] &&
			reflect.DeepEqual(current.RoleRef, desired.RoleRef) &&
			reflect.DeepEqual(current.Subjects, desired.Subjects) {
			return nil
		}
		current.Labels = desired.Labels
		current.RoleRef = desired.RoleRef
		current.Subjects = desired.Subjects
		_, updateErr := clientset.RbacV1().RoleBindings(namespace).Update(ctx, current, metav1.UpdateOptions{})
		return updateErr
	})
}

func ensureServiceAccount(
	ctx context.Context,
	clientset kubernetes.Interface,
	namespace string,
	name string,
	labels map[string]string,
	resetReference bool,
) error {
	_, err := clientset.CoreV1().ServiceAccounts(namespace).Create(ctx, &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, Labels: labels},
	}, metav1.CreateOptions{})
	if err == nil {
		return nil
	}
	if !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("create ServiceAccount %s/%s: %w", namespace, name, err)
	}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		current, getErr := clientset.CoreV1().ServiceAccounts(namespace).Get(ctx, name, metav1.GetOptions{})
		if getErr != nil {
			return getErr
		}
		if current.Labels == nil {
			current.Labels = make(map[string]string)
		}
		desiredReferences := current.ImagePullSecrets
		if resetReference {
			desiredReferences = removeManagedSecretReference(current.ImagePullSecrets)
		}
		if current.Labels[performanceRunLabel] == labels[performanceRunLabel] &&
			reflect.DeepEqual(current.ImagePullSecrets, desiredReferences) {
			return nil
		}
		current.Labels[performanceRunLabel] = labels[performanceRunLabel]
		current.ImagePullSecrets = desiredReferences
		_, updateErr := clientset.CoreV1().ServiceAccounts(namespace).Update(ctx, current, metav1.UpdateOptions{})
		return updateErr
	})
}

func removeManagedSecretReference(references []corev1.LocalObjectReference) []corev1.LocalObjectReference {
	desired := make([]corev1.LocalObjectReference, 0, len(references))
	for _, reference := range references {
		if reference.Name != controller.DefaultManagedSecretName {
			desired = append(desired, reference)
		}
	}
	return desired
}

func resetManagedSecrets(
	ctx context.Context,
	clientset kubernetes.Interface,
	namespaces []string,
	concurrency int,
) error {
	targets := make(map[string]struct{}, len(namespaces))
	for _, namespace := range namespaces {
		targets[namespace] = struct{}{}
	}
	secrets, err := clientset.CoreV1().Secrets(metav1.NamespaceAll).List(ctx, metav1.ListOptions{
		FieldSelector: fields.OneTermEqualSelector("metadata.name", controller.DefaultManagedSecretName).String(),
	})
	if err != nil {
		return err
	}
	toDelete := make([]string, 0, len(secrets.Items))
	for index := range secrets.Items {
		if _, wanted := targets[secrets.Items[index].Namespace]; wanted {
			toDelete = append(toDelete, secrets.Items[index].Namespace)
		}
	}
	return runParallel(ctx, toDelete, concurrency, func(ctx context.Context, namespace string) error {
		err := clientset.CoreV1().Secrets(namespace).Delete(
			ctx,
			controller.DefaultManagedSecretName,
			metav1.DeleteOptions{},
		)
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	})
}

func configureTargets(
	ctx context.Context,
	clientset kubernetes.Interface,
	namespaces []string,
	serviceAccounts []string,
) error {
	namespaceValue := strings.Join(namespaces, ",")
	if len(namespaceValue) > maximumConfigMapDataSize {
		return fmt.Errorf("namespace selector is %d bytes and exceeds the safe ConfigMap data limit", len(namespaceValue))
	}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		configMap, err := clientset.CoreV1().ConfigMaps(controllerNamespace).Get(
			ctx,
			controller.DefaultConfigMapName,
			metav1.GetOptions{},
		)
		if err != nil {
			return err
		}
		configMap.Data = map[string]string{
			config.NamespaceKey:      namespaceValue,
			config.ServiceAccountKey: strings.Join(serviceAccounts, ","),
			config.RegistriesKey: `- regionID: mock-region
  instanceID: mock-registry
  accessKeyID: mock-access-key
  accessKeySecret: mock-access-secret
  domains:
    - registry.mock.example
`,
		}
		_, err = clientset.CoreV1().ConfigMaps(controllerNamespace).Update(ctx, configMap, metav1.UpdateOptions{})
		return err
	})
}

func setDeploymentReplicas(ctx context.Context, clientset kubernetes.Interface, replicas int32) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		deployment, err := clientset.AppsV1().Deployments(controllerNamespace).Get(
			ctx,
			controllerDeployment,
			metav1.GetOptions{},
		)
		if err != nil {
			return fmt.Errorf("get performance Deployment: %w", err)
		}
		if deployment.Spec.Replicas != nil && *deployment.Spec.Replicas == replicas {
			return nil
		}
		deployment.Spec.Replicas = ptr.To(replicas)
		_, err = clientset.AppsV1().Deployments(controllerNamespace).Update(ctx, deployment, metav1.UpdateOptions{})
		return err
	})
}

func waitForControllerPods(
	ctx context.Context,
	clientset kubernetes.Interface,
	wantReady bool,
	timeout time.Duration,
) error {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	selector := "app.kubernetes.io/name=" + controllerApplication
	for {
		pods, err := clientset.CoreV1().Pods(controllerNamespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
		if err != nil {
			return fmt.Errorf("list performance controller Pods: %w", err)
		}
		if !wantReady && len(pods.Items) == 0 {
			return nil
		}
		if wantReady && len(pods.Items) == 1 && podReady(&pods.Items[0]) {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return fmt.Errorf("controller Pods did not reach wantReady=%t within %s", wantReady, timeout)
		case <-ticker.C:
		}
	}
}

func podReady(pod *corev1.Pod) bool {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func advanceControllerClock(ctx context.Context, clientset kubernetes.Interface, duration time.Duration) error {
	raw, err := clientset.CoreV1().RESTClient().Post().
		Namespace(controllerNamespace).
		Resource("services").
		Name("http:"+clockControlService+":"+clockControlServicePort).
		SubResource("proxy").
		Suffix("advance").
		Param("duration", duration.String()).
		Do(ctx).
		Raw()
	if err != nil {
		return fmt.Errorf("advance mock scheduler clock: %w", err)
	}
	fmt.Printf("EXPIRY_TRIGGER %s\n", strings.TrimSpace(string(raw)))
	return nil
}

func waitForPhase(
	ctx context.Context,
	clientset kubernetes.Interface,
	tracker *convergenceTracker,
	phase *trackedPhase,
	resources *resourceRecorder,
	progressPath string,
	timeout time.Duration,
) error {
	recorder, err := newProgressRecorder(progressPath)
	if err != nil {
		return err
	}
	defer recorder.Close()
	writeProgress := func() error {
		return recorder.Write(tracker.progress(phase, time.Now()))
	}
	if err := writeProgress(); err != nil {
		return fmt.Errorf("record initial progress: %w", err)
	}

	sampleTicker := time.NewTicker(progressSampleInterval)
	defer sampleTicker.Stop()
	logTicker := time.NewTicker(progressLogInterval)
	defer logTicker.Stop()
	resourceTicker := time.NewTicker(5 * time.Second)
	defer resourceTicker.Stop()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		select {
		case <-phase.done:
			resources.Sample(ctx, clientset, phase.kind)
			if err := writeProgress(); err != nil {
				return fmt.Errorf("record final progress: %w", err)
			}
			progress := tracker.progress(phase, time.Now())
			fmt.Printf(
				"COMPLETE phase=%s elapsed=%.3fs secrets=%d/%d serviceaccounts=%d/%d unexpected_sa_updates=%d\n",
				phase.kind,
				progress.Elapsed.Seconds(),
				progress.SecretsComplete,
				progress.SecretsTotal,
				progress.ServiceAccountsComplete,
				progress.ServiceAccountsTotal,
				progress.UnexpectedServiceAccounts,
			)
			return nil
		case <-sampleTicker.C:
			if err := writeProgress(); err != nil {
				return fmt.Errorf("record progress: %w", err)
			}
		case <-logTicker.C:
			progress := tracker.progress(phase, time.Now())
			fmt.Printf(
				"PROGRESS phase=%s elapsed=%.1fs secrets=%d/%d serviceaccounts=%d/%d unexpected_sa_updates=%d\n",
				phase.kind,
				progress.Elapsed.Seconds(),
				progress.SecretsComplete,
				progress.SecretsTotal,
				progress.ServiceAccountsComplete,
				progress.ServiceAccountsTotal,
				progress.UnexpectedServiceAccounts,
			)
		case <-resourceTicker.C:
			resources.Sample(ctx, clientset, phase.kind)
		case <-deadline.C:
			return fmt.Errorf("phase %s did not converge within %s", phase.kind, timeout)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func verifyFinalState(
	ctx context.Context,
	clientset kubernetes.Interface,
	namespaces []string,
	runLabelSelector string,
) error {
	targets := make(map[string]struct{}, len(namespaces))
	for _, namespace := range namespaces {
		targets[namespace] = struct{}{}
	}
	secrets, err := clientset.CoreV1().Secrets(metav1.NamespaceAll).List(ctx, metav1.ListOptions{
		FieldSelector: fields.OneTermEqualSelector("metadata.name", controller.DefaultManagedSecretName).String(),
	})
	if err != nil {
		return fmt.Errorf("verify Secrets: %w", err)
	}
	validSecrets := 0
	for index := range secrets.Items {
		secret := &secrets.Items[index]
		if _, wanted := targets[secret.Namespace]; wanted && registrysecret.IsUsableForServiceAccount(secret) {
			validSecrets++
		}
	}
	if validSecrets != len(namespaces) {
		return fmt.Errorf("final valid Secret count = %d, want %d", validSecrets, len(namespaces))
	}
	accounts, err := clientset.CoreV1().ServiceAccounts(metav1.NamespaceAll).List(ctx, metav1.ListOptions{
		LabelSelector: runLabelSelector,
	})
	if err != nil {
		return fmt.Errorf("verify ServiceAccounts: %w", err)
	}
	validAccounts := 0
	for index := range accounts.Items {
		account := &accounts.Items[index]
		if _, wantedNamespace := targets[account.Namespace]; wantedNamespace && containsString(targetServiceAccounts, account.Name) && referencesManagedSecret(account) {
			validAccounts++
		}
	}
	wantAccounts := len(namespaces) * len(targetServiceAccounts)
	if validAccounts != wantAccounts {
		return fmt.Errorf("final patched ServiceAccount count = %d, want %d", validAccounts, wantAccounts)
	}
	return nil
}

func containsString(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

func cleanupRun(
	ctx context.Context,
	clientset kubernetes.Interface,
	runID string,
	concurrency int,
) error {
	selector := performanceRunLabel + "=" + runID
	namespaceList, err := clientset.CoreV1().Namespaces().List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return fmt.Errorf("list run Namespaces: %w", err)
	}
	names := make([]string, 0, len(namespaceList.Items))
	for index := range namespaceList.Items {
		names = append(names, namespaceList.Items[index].Name)
	}
	if len(names) == 0 {
		return nil
	}
	fmt.Printf("CLEANUP deleting_namespaces=%d run=%s\n", len(names), runID)
	if err := runParallel(ctx, names, concurrency, func(ctx context.Context, name string) error {
		err := clientset.CoreV1().Namespaces().Delete(ctx, name, metav1.DeleteOptions{
			PropagationPolicy: ptr.To(metav1.DeletePropagationBackground),
		})
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}); err != nil {
		return err
	}
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		remaining, err := clientset.CoreV1().Namespaces().List(ctx, metav1.ListOptions{LabelSelector: selector})
		if err != nil {
			return err
		}
		if len(remaining.Items) == 0 {
			fmt.Printf("CLEANUP complete run=%s\n", runID)
			return nil
		}
		fmt.Printf("CLEANUP remaining_namespaces=%d run=%s\n", len(remaining.Items), runID)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func runParallel(
	ctx context.Context,
	items []string,
	workers int,
	operation func(context.Context, string) error,
) error {
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan string)
	errCh := make(chan error, 1)
	var group sync.WaitGroup
	for range min(workers, max(1, len(items))) {
		group.Add(1)
		go func() {
			defer group.Done()
			for item := range jobs {
				if err := operation(workerCtx, item); err != nil {
					select {
					case errCh <- err:
						cancel()
					default:
					}
					return
				}
			}
		}()
	}

	go func() {
		defer close(jobs)
		for _, item := range items {
			select {
			case jobs <- item:
			case <-workerCtx.Done():
				return
			}
		}
	}()
	group.Wait()
	select {
	case err := <-errCh:
		return err
	default:
	}
	return ctx.Err()
}

func writeSummary(options commandOptions, summary benchmarkSummary) error {
	data, err := json.MarshalIndent(summary, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	path := filepath.Join(options.outputDirectory, options.runID+"-summary.json")
	return os.WriteFile(path, data, 0o644)
}
