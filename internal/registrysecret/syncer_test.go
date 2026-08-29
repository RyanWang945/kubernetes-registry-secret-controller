package registrysecret

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/RyanWang945/kubernetes-registry-secret-controller/internal/config"
	"github.com/RyanWang945/kubernetes-registry-secret-controller/internal/credential"
)

func TestSyncerCreatesRepairsAndDeletesManagedSecret(t *testing.T) {
	t.Parallel()

	registry := testRegistry("cn-hangzhou", "cri-a", "key", "secret", "a.example.com")
	configStore := &config.Store{}
	configStore.Apply(testSnapshot(registry))
	credentialStore := &credential.Store{}
	credentialStore.Apply(registry, testCredential("user", "password", time.Now()))
	baseClient := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "production"}},
	).Build()
	recorder := &recordingClient{Client: baseClient}
	syncer, err := NewSyncer(recorder, configStore, credentialStore, "auto-patch-secret")
	if err != nil {
		t.Fatalf("NewSyncer() error = %v", err)
	}

	if err := syncer.SyncNamespaceSecret(context.Background(), "production"); err != nil {
		t.Fatalf("create SyncNamespaceSecret() error = %v", err)
	}
	secret := getSecret(t, baseClient, "production", "auto-patch-secret")
	if !IsManaged(secret) || secret.Type != corev1.SecretTypeDockerConfigJson {
		t.Fatalf("created Secret metadata/type = %+v, %s", secret.Labels, secret.Type)
	}
	if got := decodeDockerConfig(t, secret.Data[corev1.DockerConfigJsonKey]).Auths["a.example.com"].Password; got != "password" {
		t.Fatalf("created Secret password = %q, want password", got)
	}
	if recorder.createCount() != 1 {
		t.Fatalf("Secret creates = %d, want 1", recorder.createCount())
	}

	if err := syncer.SyncNamespaceSecret(context.Background(), "production"); err != nil {
		t.Fatalf("idempotent SyncNamespaceSecret() error = %v", err)
	}
	if recorder.updateCount() != 0 {
		t.Fatalf("idempotent Secret updates = %d, want 0", recorder.updateCount())
	}

	secret.Labels["user-label"] = "preserved"
	secret.Type = corev1.SecretTypeOpaque
	secret.Data = map[string][]byte{"drift": []byte("bad")}
	secret.Annotations[StateAnnotationKey] = "bad-state"
	if err := baseClient.Update(context.Background(), secret); err != nil {
		t.Fatalf("introduce Secret drift: %v", err)
	}
	if err := syncer.SyncNamespaceSecret(context.Background(), "production"); err != nil {
		t.Fatalf("repair SyncNamespaceSecret() error = %v", err)
	}
	repaired := getSecret(t, baseClient, "production", "auto-patch-secret")
	if repaired.Type != corev1.SecretTypeDockerConfigJson || repaired.Labels["user-label"] != "preserved" {
		t.Fatalf("repaired Secret type/labels = %s, %+v", repaired.Type, repaired.Labels)
	}
	if len(repaired.Data) != 1 || repaired.Data[corev1.DockerConfigJsonKey] == nil {
		t.Fatalf("repaired Secret data = %+v, want only dockerconfigjson", repaired.Data)
	}

	configStore.Apply(config.ConfigurationSnapshot{
		Namespaces:      config.NameSelector{Names: []string{"staging"}},
		ServiceAccounts: config.NameSelector{MatchAll: true},
		Registries:      map[config.RegistryKey]config.RegistryConfig{registry.Key(): registry},
	})
	if err := syncer.SyncNamespaceSecret(context.Background(), "production"); err != nil {
		t.Fatalf("cleanup SyncNamespaceSecret() error = %v", err)
	}
	if err := baseClient.Get(context.Background(), client.ObjectKey{Namespace: "production", Name: "auto-patch-secret"}, &corev1.Secret{}); !apierrors.IsNotFound(err) {
		t.Fatalf("managed Secret after cleanup error = %v, want NotFound", err)
	}
}

func TestSyncerDoesNotOverwriteUnownedSecretOrCreateInMissingNamespace(t *testing.T) {
	t.Parallel()

	registry := testRegistry("cn-hangzhou", "cri-a", "key", "secret", "a.example.com")
	configStore := &config.Store{}
	configStore.Apply(testSnapshot(registry))
	unowned := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "auto-patch-secret", Namespace: "production"},
		Data:       map[string][]byte{"user-data": []byte("preserve")},
	}
	baseClient := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "production"}},
		unowned,
	).Build()
	recorder := &recordingClient{Client: baseClient}
	syncer, err := NewSyncer(recorder, configStore, &credential.Store{}, "auto-patch-secret")
	if err != nil {
		t.Fatalf("NewSyncer() error = %v", err)
	}

	if err := syncer.SyncNamespaceSecret(context.Background(), "production"); err != nil {
		t.Fatalf("unowned SyncNamespaceSecret() error = %v", err)
	}
	current := getSecret(t, baseClient, "production", "auto-patch-secret")
	if string(current.Data["user-data"]) != "preserve" || recorder.updateCount() != 0 {
		t.Fatal("unowned Secret was modified")
	}

	if err := syncer.SyncNamespaceSecret(context.Background(), "missing"); err != nil {
		t.Fatalf("missing Namespace SyncNamespaceSecret() error = %v", err)
	}
	if recorder.createCount() != 0 {
		t.Fatal("Secret was created for a missing Namespace")
	}
}

func TestSyncerNamespaceFailuresAreIndependent(t *testing.T) {
	t.Parallel()

	registry := testRegistry("cn-hangzhou", "cri-a", "key", "secret", "a.example.com")
	configStore := &config.Store{}
	configStore.Apply(testSnapshot(registry))
	credentialStore := &credential.Store{}
	credentialStore.Apply(registry, testCredential("user", "password", time.Now()))
	baseClient := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "broken"}},
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "healthy"}},
	).Build()
	syncer, err := NewSyncer(
		&failNamespaceCreateClient{Client: baseClient, namespace: "broken"},
		configStore,
		credentialStore,
		"auto-patch-secret",
	)
	if err != nil {
		t.Fatalf("NewSyncer() error = %v", err)
	}

	if err := syncer.SyncNamespaceSecret(context.Background(), "broken"); err == nil {
		t.Fatal("broken Namespace sync error = nil, want isolated create failure")
	}
	if err := syncer.SyncNamespaceSecret(context.Background(), "healthy"); err != nil {
		t.Fatalf("healthy Namespace sync error = %v", err)
	}
	getSecret(t, baseClient, "healthy", "auto-patch-secret")
	if err := baseClient.Get(context.Background(), client.ObjectKey{Namespace: "broken", Name: "auto-patch-secret"}, &corev1.Secret{}); !apierrors.IsNotFound(err) {
		t.Fatalf("broken Namespace Secret error = %v, want NotFound", err)
	}
}

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme() error = %v", err)
	}
	return scheme
}

func getSecret(t *testing.T, kubernetesClient client.Client, namespace, name string) *corev1.Secret {
	t.Helper()
	secret := &corev1.Secret{}
	if err := kubernetesClient.Get(context.Background(), client.ObjectKey{Namespace: namespace, Name: name}, secret); err != nil {
		t.Fatalf("Get() Secret error = %v", err)
	}
	return secret
}

type recordingClient struct {
	client.Client
	mu      sync.Mutex
	creates int
	updates int
	deletes int
}

func (c *recordingClient) Create(ctx context.Context, object client.Object, options ...client.CreateOption) error {
	c.mu.Lock()
	c.creates++
	c.mu.Unlock()
	return c.Client.Create(ctx, object, options...)
}

func (c *recordingClient) Update(ctx context.Context, object client.Object, options ...client.UpdateOption) error {
	c.mu.Lock()
	c.updates++
	c.mu.Unlock()
	return c.Client.Update(ctx, object, options...)
}

func (c *recordingClient) Delete(ctx context.Context, object client.Object, options ...client.DeleteOption) error {
	c.mu.Lock()
	c.deletes++
	c.mu.Unlock()
	return c.Client.Delete(ctx, object, options...)
}

func (c *recordingClient) createCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.creates
}

func (c *recordingClient) updateCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.updates
}

type failNamespaceCreateClient struct {
	client.Client
	namespace string
}

func (c *failNamespaceCreateClient) Create(ctx context.Context, object client.Object, options ...client.CreateOption) error {
	if object.GetNamespace() == c.namespace {
		return errors.New("simulated namespace-specific create failure")
	}
	return c.Client.Create(ctx, object, options...)
}
