//go:build integration

package controller

import (
	"context"
	"errors"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/RyanWang945/kubernetes-registry-secret-controller/internal/config"
	"github.com/RyanWang945/kubernetes-registry-secret-controller/internal/credential"
	"github.com/RyanWang945/kubernetes-registry-secret-controller/internal/registrysecret"
	serviceaccountsyncer "github.com/RyanWang945/kubernetes-registry-secret-controller/internal/serviceaccount"
)

// Envtest assets are released independently from Kubernetes patch releases.
// v1.36.2 is the newest published control-plane bundle for the v1.36 line.
const envtestKubernetesVersion = "1.36.2"

func TestManagerInitialListAndContinuousWatch(t *testing.T) {
	assetsDirectory := filepath.Join(repositoryRoot(t), ".cache", "envtest")
	testEnvironment := &envtest.Environment{
		DownloadBinaryAssets:        true,
		DownloadBinaryAssetsVersion: envtestKubernetesVersion,
		BinaryAssetsDirectory:       assetsDirectory,
		ControlPlaneStartTimeout:    60 * time.Second,
		ControlPlaneStopTimeout:     60 * time.Second,
	}

	restConfig, err := testEnvironment.Start()
	if err != nil {
		t.Fatalf("start envtest control plane: %v", err)
	}
	t.Cleanup(func() {
		if err := testEnvironment.Stop(); err != nil {
			t.Errorf("stop envtest control plane: %v", err)
		}
	})

	scheme := k8sruntime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("register Kubernetes scheme: %v", err)
	}
	apiClient, err := client.New(restConfig, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatalf("create direct Kubernetes client: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	for _, namespace := range []string{DefaultControllerNamespace, "production"} {
		if err := apiClient.Create(ctx, &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: namespace},
		}); err != nil {
			t.Fatalf("create initial Namespace %q: %v", namespace, err)
		}
	}
	if err := apiClient.Create(ctx, testConfigMap("production,staging,retry", "default,build")); err != nil {
		t.Fatalf("create initial ConfigMap: %v", err)
	}

	cacheOptions, err := NewCacheOptions(ControllerOptions{})
	if err != nil {
		t.Fatalf("NewCacheOptions() error = %v", err)
	}
	manager, err := ctrl.NewManager(restConfig, ctrl.Options{
		Scheme:                 scheme,
		Cache:                  cacheOptions,
		Logger:                 logr.Discard(),
		LeaderElection:         false,
		Metrics:                metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress: "0",
	})
	if err != nil {
		t.Fatalf("create Manager: %v", err)
	}

	retryErr := errors.New("temporary sync failure")
	store := &config.Store{}
	namespaceRecorder := newRecordingSyncer(func(call int, namespace string) error {
		if namespace == "retry" && call == 1 {
			return retryErr
		}
		return nil
	})
	serviceAccountRecorder := newRecordingSyncer(nil)
	resourceEvents, err := NewResourceEventPublisher(manager.GetClient())
	if err != nil {
		t.Fatalf("NewResourceEventPublisher() error = %v", err)
	}
	if err := SetupWithManager(
		manager,
		store,
		&recordingConfigurationObserver{},
		resourceEvents,
		namespaceRecorder,
		serviceAccountRecorder,
		ControllerOptions{},
	); err != nil {
		t.Fatalf("SetupWithManager() error = %v", err)
	}

	runDone := make(chan error, 1)
	go func() { runDone <- manager.Start(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-runDone:
			if err != nil {
				t.Errorf("Manager.Start() error = %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Error("Manager did not stop within 10 seconds")
		}
	})

	if !manager.GetCache().WaitForCacheSync(ctx) {
		t.Fatal("Manager cache did not synchronize")
	}
	namespaceRecorder.waitForCount(t, "production", 1)
	eventually(t, 10*time.Second, func() bool {
		snapshot, ok := store.Load()
		return ok && snapshot.Generation == 1
	}, "initial ConfigMap was not reconciled through the Manager cache")

	if err := apiClient.Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: "staging"},
	}); err != nil {
		t.Fatalf("create watched Namespace: %v", err)
	}
	namespaceRecorder.waitForCount(t, "staging", 1)

	if err := apiClient.Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: "retry"},
	}); err != nil {
		t.Fatalf("create retry Namespace: %v", err)
	}
	namespaceRecorder.waitForCount(t, "retry", 2)
	if namespaceRecorder.successes("retry") != 1 {
		t.Fatalf("successful retry reconciliations = %d, want 1", namespaceRecorder.successes("retry"))
	}

	serviceAccount := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{Name: "build", Namespace: "production"},
	}
	productionBefore := namespaceRecorder.count("production")
	if err := apiClient.Create(ctx, serviceAccount); err != nil {
		t.Fatalf("create watched ServiceAccount: %v", err)
	}
	serviceAccountRecorder.waitForCount(t, "production/build", 1)
	if namespaceRecorder.count("production") != productionBefore {
		t.Fatal("ServiceAccount creation triggered namespace-wide reconciliation")
	}

	serviceAccountBefore := serviceAccountRecorder.count("production/build")
	serviceAccount.ImagePullSecrets = []corev1.LocalObjectReference{{Name: "manually-added"}}
	if err := apiClient.Update(ctx, serviceAccount); err != nil {
		t.Fatalf("update watched ServiceAccount: %v", err)
	}
	serviceAccountRecorder.waitForCount(t, "production/build", serviceAccountBefore+1)
	if namespaceRecorder.count("production") != productionBefore {
		t.Fatal("ServiceAccount update triggered namespace-wide reconciliation")
	}

	serviceAccountBefore = serviceAccountRecorder.count("production/build")
	if err := apiClient.Create(ctx, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: DefaultManagedSecretName, Namespace: "production"},
	}); err != nil {
		t.Fatalf("create watched managed Secret: %v", err)
	}
	namespaceRecorder.waitForCount(t, "production", productionBefore+1)
	serviceAccountRecorder.waitForCount(t, "production/build", serviceAccountBefore+1)

	configMap := &corev1.ConfigMap{}
	configMapKey := client.ObjectKey{Namespace: DefaultControllerNamespace, Name: DefaultConfigMapName}
	if err := apiClient.Get(ctx, configMapKey, configMap); err != nil {
		t.Fatalf("get ConfigMap for invalid update: %v", err)
	}
	configMap.Data[config.NamespaceKey] = "production,,staging"
	if err := apiClient.Update(ctx, configMap); err != nil {
		t.Fatalf("update invalid ConfigMap: %v", err)
	}
	waitForCachedConfigMapResourceVersion(t, manager.GetClient(), configMapKey, configMap.ResourceVersion)
	unchanged, ok := store.Load()
	if !ok || unchanged.Generation != 1 || !unchanged.MatchesNamespace("staging") {
		t.Fatalf("invalid update replaced the last valid snapshot: %+v, present = %v", unchanged, ok)
	}

	if err := apiClient.Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: "future"},
	}); err != nil {
		t.Fatalf("create future Namespace: %v", err)
	}
	namespaceRecorder.waitForCount(t, "future", 1)
	futureBefore := namespaceRecorder.count("future")
	serviceAccountBefore = serviceAccountRecorder.count("production/build")

	configMap.Data[config.NamespaceKey] = "production,staging,retry,future"
	if err := apiClient.Update(ctx, configMap); err != nil {
		t.Fatalf("update valid ConfigMap: %v", err)
	}
	eventually(t, 10*time.Second, func() bool {
		snapshot, ok := store.Load()
		return ok && snapshot.Generation == 2 && snapshot.MatchesNamespace("future")
	}, "valid ConfigMap update did not install generation 2")
	namespaceRecorder.waitForCount(t, "future", futureBefore+1)
	serviceAccountRecorder.waitForCount(t, "production/build", serviceAccountBefore+1)

	// Exercise the concrete resource syncers against a real API server. The
	// Manager uses recording syncers above to observe queue behavior, while these
	// direct calls verify the cache-backed readiness gate and actual SA Patch.
	snapshot, _ := store.Load()
	credentialStore := &credential.Store{}
	secretSyncer, err := registrysecret.NewSyncer(
		manager.GetClient(),
		store,
		credentialStore,
		DefaultManagedSecretName,
		manager.GetEventRecorderFor("envtest-namespace-secrets"),
	)
	if err != nil {
		t.Fatalf("create concrete Secret syncer: %v", err)
	}
	if err := secretSyncer.SyncNamespaceSecret(ctx, "future"); err != nil {
		t.Fatalf("sync concrete Secret without credentials: %v", err)
	}
	secretKey := client.ObjectKey{Namespace: "future", Name: DefaultManagedSecretName}
	if err := apiClient.Get(ctx, secretKey, &corev1.Secret{}); !apierrors.IsNotFound(err) {
		t.Fatalf("empty managed Secret error = %v, want NotFound", err)
	}

	waitingServiceAccount := &corev1.ServiceAccount{
		ObjectMeta:       metav1.ObjectMeta{Name: "build", Namespace: "future"},
		ImagePullSecrets: []corev1.LocalObjectReference{{Name: "user-secret"}},
	}
	stableServiceAccount := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: "future"},
		ImagePullSecrets: []corev1.LocalObjectReference{
			{Name: "user-secret"},
			{Name: DefaultManagedSecretName},
		},
	}
	waitingBefore := serviceAccountRecorder.count("future/build")
	stableBefore := serviceAccountRecorder.count("future/default")
	for _, serviceAccount := range []*corev1.ServiceAccount{waitingServiceAccount, stableServiceAccount} {
		if err := apiClient.Create(ctx, serviceAccount); err != nil {
			t.Fatalf("create concrete-sync ServiceAccount %s: %v", client.ObjectKeyFromObject(serviceAccount), err)
		}
		eventually(t, 10*time.Second, func() bool {
			return manager.GetClient().Get(
				ctx,
				client.ObjectKeyFromObject(serviceAccount),
				&corev1.ServiceAccount{},
			) == nil
		}, "concrete-sync ServiceAccount was not observed through the Manager cache")
	}
	serviceAccountRecorder.waitForCount(t, "future/build", waitingBefore+1)
	serviceAccountRecorder.waitForCount(t, "future/default", stableBefore+1)

	serviceAccountSyncer, err := serviceaccountsyncer.NewSyncer(
		manager.GetClient(),
		store,
		DefaultManagedSecretName,
	)
	if err != nil {
		t.Fatalf("create concrete ServiceAccount syncer: %v", err)
	}
	for _, serviceAccount := range []*corev1.ServiceAccount{waitingServiceAccount, stableServiceAccount} {
		if err := serviceAccountSyncer.SyncServiceAccount(
			ctx,
			client.ObjectKeyFromObject(serviceAccount),
		); !errors.Is(err, serviceaccountsyncer.ErrManagedSecretNotReady) {
			t.Fatalf("sync ServiceAccount %s without Secret error = %v, want ErrManagedSecretNotReady", client.ObjectKeyFromObject(serviceAccount), err)
		}
	}
	if !serviceAccountHasReferences(ctx, apiClient, client.ObjectKeyFromObject(waitingServiceAccount), "user-secret") {
		t.Fatal("missing Secret changed the waiting ServiceAccount references")
	}
	if !serviceAccountHasReferences(
		ctx,
		apiClient,
		client.ObjectKeyFromObject(stableServiceAccount),
		"user-secret",
		DefaultManagedSecretName,
	) {
		t.Fatal("missing Secret removed the stable managed reference")
	}

	for _, registry := range snapshot.Registries {
		credentialStore.Apply(registry, credential.Credential{
			Username:    "envtest-user",
			Password:    "envtest-password",
			RefreshedAt: time.Now().UTC(),
			ExpiresAt:   time.Now().UTC().Add(time.Hour),
		})
	}
	waitingBeforeSecret := serviceAccountRecorder.count("future/build")
	stableBeforeSecret := serviceAccountRecorder.count("future/default")
	if err := secretSyncer.SyncNamespaceSecret(ctx, "future"); err != nil {
		t.Fatalf("sync concrete Secret: %v", err)
	}
	eventually(t, 10*time.Second, func() bool {
		secret := &corev1.Secret{}
		return apiClient.Get(ctx, secretKey, secret) == nil &&
			secret.Type == corev1.SecretTypeDockerConfigJson &&
			registrysecret.IsManaged(secret)
	}, "concrete Secret syncer did not create the managed dockerconfigjson Secret")
	serviceAccountRecorder.waitForCount(t, "future/build", waitingBeforeSecret+1)
	serviceAccountRecorder.waitForCount(t, "future/default", stableBeforeSecret+1)

	waitingBeforePatch := serviceAccountRecorder.count("future/build")
	if err := serviceAccountSyncer.SyncServiceAccount(ctx, client.ObjectKeyFromObject(waitingServiceAccount)); err != nil {
		t.Fatalf("sync concrete ServiceAccount: %v", err)
	}
	eventually(t, 10*time.Second, func() bool {
		return serviceAccountHasReferences(
			ctx,
			apiClient,
			client.ObjectKeyFromObject(waitingServiceAccount),
			"user-secret",
			DefaultManagedSecretName,
		)
	}, "concrete ServiceAccount syncer did not preserve and inject imagePullSecrets")
	serviceAccountRecorder.waitForCount(t, "future/build", waitingBeforePatch+1)

	managedSecret := &corev1.Secret{}
	if err := apiClient.Get(ctx, secretKey, managedSecret); err != nil {
		t.Fatalf("get managed Secret for event predicate checks: %v", err)
	}
	ordinaryUpdateCount := serviceAccountRecorder.count("future/build")
	managedSecret.Data[corev1.DockerConfigJsonKey] = []byte(`{"auths":{"registry.example.com":{}}}`)
	if err := apiClient.Update(ctx, managedSecret); err != nil {
		t.Fatalf("update managed Secret data: %v", err)
	}
	waitForCachedSecretResourceVersion(t, manager.GetClient(), secretKey, managedSecret.ResourceVersion)
	time.Sleep(200 * time.Millisecond)
	if got := serviceAccountRecorder.count("future/build"); got != ordinaryUpdateCount {
		t.Fatalf("ordinary Secret update reconciliations = %d, want unchanged %d", got, ordinaryUpdateCount)
	}

	delete(managedSecret.Labels, registrysecret.ManagedByLabelKey)
	if err := apiClient.Update(ctx, managedSecret); err != nil {
		t.Fatalf("remove managed Secret ownership label: %v", err)
	}
	serviceAccountRecorder.waitForCount(t, "future/build", ordinaryUpdateCount+1)
	managedSecret.Labels[registrysecret.ManagedByLabelKey] = registrysecret.ControllerIdentity
	if err := apiClient.Update(ctx, managedSecret); err != nil {
		t.Fatalf("restore managed Secret ownership label: %v", err)
	}
	serviceAccountRecorder.waitForCount(t, "future/build", ordinaryUpdateCount+2)

	unrelated := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "unrelated", Namespace: DefaultControllerNamespace},
	}
	if err := apiClient.Create(ctx, unrelated); err != nil {
		t.Fatalf("create unrelated ConfigMap: %v", err)
	}
	if err := apiClient.Get(ctx, client.ObjectKeyFromObject(unrelated), &corev1.ConfigMap{}); err != nil {
		t.Fatalf("direct client cannot read unrelated ConfigMap: %v", err)
	}
	if err := manager.GetClient().Get(ctx, client.ObjectKeyFromObject(unrelated), &corev1.ConfigMap{}); !apierrors.IsNotFound(err) {
		t.Fatalf("cached client unrelated ConfigMap error = %v, want NotFound from fixed-name cache", err)
	}

	if err := apiClient.Delete(ctx, configMap); err != nil {
		t.Fatalf("delete fixed ConfigMap: %v", err)
	}
	eventually(t, 10*time.Second, func() bool {
		err := manager.GetClient().Get(ctx, configMapKey, &corev1.ConfigMap{})
		return apierrors.IsNotFound(err)
	}, "fixed ConfigMap deletion was not observed by the Manager cache")
	afterDelete, ok := store.Load()
	if !ok || afterDelete.Generation != 2 || !afterDelete.MatchesNamespace("future") {
		t.Fatalf("ConfigMap deletion removed the last valid snapshot: %+v, present = %v", afterDelete, ok)
	}
}

func waitForCachedConfigMapResourceVersion(
	t *testing.T,
	kubernetesClient client.Client,
	key client.ObjectKey,
	resourceVersion string,
) {
	t.Helper()
	eventually(t, 10*time.Second, func() bool {
		configMap := &corev1.ConfigMap{}
		return kubernetesClient.Get(context.Background(), key, configMap) == nil &&
			configMap.ResourceVersion == resourceVersion
	}, "ConfigMap resourceVersion was not observed through the Manager cache")
}

func waitForCachedSecretResourceVersion(
	t *testing.T,
	kubernetesClient client.Client,
	key client.ObjectKey,
	resourceVersion string,
) {
	t.Helper()
	eventually(t, 10*time.Second, func() bool {
		secret := &corev1.Secret{}
		return kubernetesClient.Get(context.Background(), key, secret) == nil &&
			secret.ResourceVersion == resourceVersion
	}, "Secret resourceVersion was not observed through the Manager cache")
}

func serviceAccountHasReferences(
	ctx context.Context,
	kubernetesClient client.Client,
	key client.ObjectKey,
	expected ...string,
) bool {
	serviceAccount := &corev1.ServiceAccount{}
	if err := kubernetesClient.Get(ctx, key, serviceAccount); err != nil || len(serviceAccount.ImagePullSecrets) != len(expected) {
		return false
	}
	for index, name := range expected {
		if serviceAccount.ImagePullSecrets[index].Name != name {
			return false
		}
	}
	return true
}

func eventually(t *testing.T, timeout time.Duration, condition func() bool, failureMessage string) {
	t.Helper()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
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

func repositoryRoot(t *testing.T) string {
	t.Helper()
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve integration test source path")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(filename), "..", ".."))
}
