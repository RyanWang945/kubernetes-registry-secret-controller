//go:build integration

package controller

import (
	"context"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	"github.com/RyanWang945/kubernetes-registry-secret-controller/internal/config"
)

// Envtest assets are released independently from Kubernetes patch releases.
// v1.36.2 is the newest published control-plane bundle for the v1.36 line.
const envtestKubernetesVersion = "1.36.2"

func TestControllerEnvtestInitialListAndContinuousWatch(t *testing.T) {
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

	client, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		t.Fatalf("create Kubernetes client: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	for _, namespace := range []string{DefaultControllerNamespace, "production"} {
		if _, err := client.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: namespace},
		}, metav1.CreateOptions{}); err != nil {
			t.Fatalf("create initial Namespace %q: %v", namespace, err)
		}
	}
	if _, err := client.CoreV1().ConfigMaps(DefaultControllerNamespace).Create(
		ctx,
		testConfigMap("production,staging", "default,build"),
		metav1.CreateOptions{},
	); err != nil {
		t.Fatalf("create initial ConfigMap: %v", err)
	}

	store := &config.Store{}
	recorder := newRecordingSyncer(nil)
	resourceController, err := New(client, store, recorder, Options{Logger: testLogger()})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- resourceController.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-runDone:
			if err != nil {
				t.Errorf("Run() error = %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Error("Run() did not stop within 10 seconds")
		}
	})

	waitForSignal(t, resourceController.CachesSynced(), "envtest informer cache synchronization")
	recorder.waitForCount(t, "production", 1)
	eventually(t, 5*time.Second, func() bool {
		snapshot, ok := store.Load()
		return ok && snapshot.Generation == 1
	}, "initial ConfigMap was not loaded through the informer List")

	if _, err := client.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: "staging"},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create watched Namespace: %v", err)
	}
	recorder.waitForCount(t, "staging", 1)

	productionBefore := recorder.count("production")
	serviceAccount, err := client.CoreV1().ServiceAccounts("production").Create(ctx, &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{Name: "build", Namespace: "production"},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("create watched ServiceAccount: %v", err)
	}
	recorder.waitForCount(t, "production", productionBefore+1)

	productionBefore = recorder.count("production")
	serviceAccount.ImagePullSecrets = []corev1.LocalObjectReference{{Name: "manually-added"}}
	if _, err := client.CoreV1().ServiceAccounts("production").Update(
		ctx,
		serviceAccount,
		metav1.UpdateOptions{},
	); err != nil {
		t.Fatalf("update watched ServiceAccount: %v", err)
	}
	recorder.waitForCount(t, "production", productionBefore+1)

	configMap, err := client.CoreV1().ConfigMaps(DefaultControllerNamespace).Get(
		ctx,
		DefaultConfigMapName,
		metav1.GetOptions{},
	)
	if err != nil {
		t.Fatalf("get ConfigMap for invalid update: %v", err)
	}
	configMap.Data[config.NamespaceKey] = "production,,staging"
	invalidUpdate, err := client.CoreV1().ConfigMaps(DefaultControllerNamespace).Update(
		ctx,
		configMap,
		metav1.UpdateOptions{},
	)
	if err != nil {
		t.Fatalf("update invalid ConfigMap: %v", err)
	}
	waitForConfigMapResourceVersion(t, resourceController, invalidUpdate.ResourceVersion)
	unchanged, ok := store.Load()
	if !ok || unchanged.Generation != 1 || !unchanged.Namespaces.Matches("staging") {
		t.Fatalf("invalid update replaced the last valid snapshot: %+v, present = %v", unchanged, ok)
	}

	if _, err := client.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: "future"},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create future Namespace: %v", err)
	}

	configMap = invalidUpdate.DeepCopy()
	configMap.Data[config.NamespaceKey] = "production,staging,future"
	validUpdate, err := client.CoreV1().ConfigMaps(DefaultControllerNamespace).Update(
		ctx,
		configMap,
		metav1.UpdateOptions{},
	)
	if err != nil {
		t.Fatalf("update valid ConfigMap: %v", err)
	}
	waitForConfigMapResourceVersion(t, resourceController, validUpdate.ResourceVersion)
	eventually(t, 5*time.Second, func() bool {
		snapshot, ok := store.Load()
		return ok && snapshot.Generation == 2 && snapshot.Namespaces.Matches("future")
	}, "valid ConfigMap Watch update did not install generation 2")
	recorder.waitForCount(t, "future", 1)

	if err := client.CoreV1().ConfigMaps(DefaultControllerNamespace).Delete(
		ctx,
		DefaultConfigMapName,
		metav1.DeleteOptions{},
	); err != nil {
		t.Fatalf("delete ConfigMap: %v", err)
	}
	eventually(t, 5*time.Second, func() bool {
		_, err := resourceController.configMapInformer.Lister().ConfigMaps(DefaultControllerNamespace).Get(DefaultConfigMapName)
		return apierrors.IsNotFound(err)
	}, "ConfigMap delete Watch event was not reflected in the informer cache")

	afterDelete, ok := store.Load()
	if !ok || afterDelete.Generation != 2 || !afterDelete.Namespaces.Matches("future") {
		t.Fatalf("ConfigMap deletion removed the last valid snapshot: %+v, present = %v", afterDelete, ok)
	}
}

func waitForConfigMapResourceVersion(t *testing.T, resourceController *Controller, resourceVersion string) {
	t.Helper()
	eventually(t, 5*time.Second, func() bool {
		configMap, err := resourceController.configMapInformer.Lister().
			ConfigMaps(DefaultControllerNamespace).
			Get(DefaultConfigMapName)
		return err == nil && configMap.ResourceVersion == resourceVersion
	}, "ConfigMap resourceVersion was not observed through Watch")
}

func repositoryRoot(t *testing.T) string {
	t.Helper()
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve integration test source path")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(filename), "..", ".."))
}
