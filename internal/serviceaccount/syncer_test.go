package serviceaccount

import (
	"context"
	"errors"
	"sync"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/RyanWang945/kubernetes-registry-secret-controller/internal/config"
	"github.com/RyanWang945/kubernetes-registry-secret-controller/internal/registrysecret"
)

const managedSecretName = "auto-patch-secret"

func TestSyncerInjectsDeduplicatesAndCleansReference(t *testing.T) {
	t.Parallel()

	configStore := &config.Store{}
	configStore.Apply(testConfiguration("production", "build"))
	baseClient := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(
		managedSecret("production"),
		&corev1.ServiceAccount{
			ObjectMeta: metav1.ObjectMeta{Name: "build", Namespace: "production"},
			ImagePullSecrets: []corev1.LocalObjectReference{
				{Name: "user-secret"},
				{Name: managedSecretName},
				{Name: managedSecretName},
			},
		},
	).Build()
	recorder := &recordingPatchClient{Client: baseClient}
	syncer, err := NewSyncer(recorder, configStore, managedSecretName)
	if err != nil {
		t.Fatalf("NewSyncer() error = %v", err)
	}
	key := types.NamespacedName{Namespace: "production", Name: "build"}

	if err := syncer.SyncServiceAccount(context.Background(), key); err != nil {
		t.Fatalf("inject SyncServiceAccount() error = %v", err)
	}
	assertReferences(t, getServiceAccount(t, baseClient, key), "user-secret", managedSecretName)
	if recorder.patchCount() != 1 {
		t.Fatalf("ServiceAccount patches = %d, want 1", recorder.patchCount())
	}

	if err := syncer.SyncServiceAccount(context.Background(), key); err != nil {
		t.Fatalf("idempotent SyncServiceAccount() error = %v", err)
	}
	if recorder.patchCount() != 1 {
		t.Fatalf("idempotent ServiceAccount patches = %d, want 1 total", recorder.patchCount())
	}

	configStore.Apply(testConfiguration("production", "default"))
	if err := syncer.SyncServiceAccount(context.Background(), key); err != nil {
		t.Fatalf("cleanup SyncServiceAccount() error = %v", err)
	}
	assertReferences(t, getServiceAccount(t, baseClient, key), "user-secret")
}

func TestSyncerAppendsReferenceOnlyAfterManagedSecretExists(t *testing.T) {
	t.Parallel()

	configStore := &config.Store{}
	configStore.Apply(testConfiguration("production", "build"))
	serviceAccount := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: "build", Namespace: "production"}}
	baseClient := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(serviceAccount).Build()
	recorder := &recordingPatchClient{Client: baseClient}
	syncer, err := NewSyncer(recorder, configStore, managedSecretName)
	if err != nil {
		t.Fatalf("NewSyncer() error = %v", err)
	}
	key := types.NamespacedName{Namespace: "production", Name: "build"}

	if err := syncer.SyncServiceAccount(context.Background(), key); err == nil {
		t.Fatal("SyncServiceAccount() without Secret error = nil, want retryable not-ready error")
	}
	assertReferences(t, getServiceAccount(t, baseClient, key))

	if err := baseClient.Create(context.Background(), managedSecret("production")); err != nil {
		t.Fatalf("Create() managed Secret error = %v", err)
	}
	if err := syncer.SyncServiceAccount(context.Background(), key); err != nil {
		t.Fatalf("SyncServiceAccount() after Secret creation error = %v", err)
	}
	assertReferences(t, getServiceAccount(t, baseClient, key), managedSecretName)
}

func TestSyncerDoesNotReferenceUnownedSecret(t *testing.T) {
	t.Parallel()

	configStore := &config.Store{}
	configStore.Apply(testConfiguration("production", "build"))
	baseClient := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: managedSecretName, Namespace: "production"}},
		&corev1.ServiceAccount{
			ObjectMeta:       metav1.ObjectMeta{Name: "build", Namespace: "production"},
			ImagePullSecrets: []corev1.LocalObjectReference{{Name: managedSecretName}},
		},
	).Build()
	recorder := &recordingPatchClient{Client: baseClient}
	syncer, err := NewSyncer(recorder, configStore, managedSecretName)
	if err != nil {
		t.Fatalf("NewSyncer() error = %v", err)
	}

	key := types.NamespacedName{Namespace: "production", Name: "build"}
	if err := syncer.SyncServiceAccount(context.Background(), key); err != nil {
		t.Fatalf("SyncServiceAccount() error = %v", err)
	}
	assertReferences(t, getServiceAccount(t, baseClient, key))
	if recorder.patchCount() != 1 {
		t.Fatal("controller-reserved reference was not removed for an unowned Secret")
	}
}

func TestSyncerReturnsOptimisticLockConflictForControllerRetry(t *testing.T) {
	t.Parallel()

	configStore := &config.Store{}
	configStore.Apply(testConfiguration("production", "build"))
	baseClient := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(
		managedSecret("production"),
		&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: "build", Namespace: "production"}},
	).Build()
	conflictClient := &failOncePatchClient{Client: baseClient}
	syncer, err := NewSyncer(conflictClient, configStore, managedSecretName)
	if err != nil {
		t.Fatalf("NewSyncer() error = %v", err)
	}
	key := types.NamespacedName{Namespace: "production", Name: "build"}

	if err := syncer.SyncServiceAccount(context.Background(), key); err == nil || !apierrors.IsConflict(errors.Unwrap(err)) {
		t.Fatalf("first SyncServiceAccount() error = %v, want Conflict", err)
	}
	if err := syncer.SyncServiceAccount(context.Background(), key); err != nil {
		t.Fatalf("retry SyncServiceAccount() error = %v", err)
	}
	assertReferences(t, getServiceAccount(t, baseClient, key), managedSecretName)
}

func testConfiguration(namespace, serviceAccount string) config.ConfigurationSnapshot {
	return config.ConfigurationSnapshot{
		Namespaces:      config.NameSelector{Names: []string{namespace}},
		ServiceAccounts: config.NameSelector{Names: []string{serviceAccount}},
		Registries: map[config.RegistryKey]config.RegistryConfig{
			{RegionID: "cn-hangzhou", InstanceID: "cri-test"}: {
				RegionID: "cn-hangzhou", InstanceID: "cri-test", AccessKeyID: "key", AccessKeySecret: "secret", Domains: []string{"registry.example.com"},
			},
		},
	}
}

func managedSecret(namespace string) *corev1.Secret {
	return &corev1.Secret{ObjectMeta: metav1.ObjectMeta{
		Name:      managedSecretName,
		Namespace: namespace,
		Labels: map[string]string{
			registrysecret.ApplicationNameLabelKey: registrysecret.ControllerIdentity,
			registrysecret.ManagedByLabelKey:       registrysecret.ControllerIdentity,
		},
	}}
}

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme() error = %v", err)
	}
	return scheme
}

func getServiceAccount(t *testing.T, kubernetesClient client.Client, key types.NamespacedName) *corev1.ServiceAccount {
	t.Helper()
	serviceAccount := &corev1.ServiceAccount{}
	if err := kubernetesClient.Get(context.Background(), key, serviceAccount); err != nil {
		t.Fatalf("Get() ServiceAccount error = %v", err)
	}
	return serviceAccount
}

func assertReferences(t *testing.T, serviceAccount *corev1.ServiceAccount, expected ...string) {
	t.Helper()
	if len(serviceAccount.ImagePullSecrets) != len(expected) {
		t.Fatalf("imagePullSecrets = %+v, want %v", serviceAccount.ImagePullSecrets, expected)
	}
	for index, name := range expected {
		if serviceAccount.ImagePullSecrets[index].Name != name {
			t.Fatalf("imagePullSecrets[%d] = %q, want %q", index, serviceAccount.ImagePullSecrets[index].Name, name)
		}
	}
}

type recordingPatchClient struct {
	client.Client
	mu      sync.Mutex
	patches int
}

func (c *recordingPatchClient) Patch(ctx context.Context, object client.Object, patch client.Patch, options ...client.PatchOption) error {
	c.mu.Lock()
	c.patches++
	c.mu.Unlock()
	return c.Client.Patch(ctx, object, patch, options...)
}

func (c *recordingPatchClient) patchCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.patches
}

type failOncePatchClient struct {
	client.Client
	failed bool
}

func (c *failOncePatchClient) Patch(ctx context.Context, object client.Object, patch client.Patch, options ...client.PatchOption) error {
	if !c.failed {
		c.failed = true
		return apierrors.NewConflict(
			schema.GroupResource{Resource: "serviceaccounts"},
			object.GetName(),
			errors.New("simulated conflict"),
		)
	}
	return c.Client.Patch(ctx, object, patch, options...)
}
