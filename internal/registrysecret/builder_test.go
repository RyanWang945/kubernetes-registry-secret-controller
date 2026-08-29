package registrysecret

import (
	"encoding/json"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/RyanWang945/kubernetes-registry-secret-controller/internal/config"
	"github.com/RyanWang945/kubernetes-registry-secret-controller/internal/credential"
)

func TestBuildAggregatesRegistriesAndDomainsDeterministically(t *testing.T) {
	t.Parallel()

	registryA := testRegistry("cn-hangzhou", "cri-a", "key-a", "secret-a", "a.example.com", "a-vpc.example.com")
	registryB := testRegistry("cn-shanghai", "cri-b", "key-b", "secret-b", "b.example.com")
	refreshedAt := time.Date(2026, time.August, 29, 10, 0, 0, 0, time.FixedZone("CST", 8*60*60))
	credentials := map[config.RegistryKey]credential.Entry{
		registryA.Key(): testEntry(registryA, testCredential("user-a", "password-a", refreshedAt)),
		registryB.Key(): testEntry(registryB, testCredential("user-b", "password-b", refreshedAt.Add(time.Minute))),
	}

	first, err := Build(testSnapshot(registryA, registryB), credentials, nil)
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	second, err := Build(testSnapshot(registryB, registryA), credentials, nil)
	if err != nil {
		t.Fatalf("Build() with reversed map insertion error = %v", err)
	}
	if string(first.DockerJSON) != string(second.DockerJSON) || first.StateJSON != second.StateJSON {
		t.Fatal("Build() output depends on Registry map insertion order")
	}

	dockerConfig := decodeDockerConfig(t, first.DockerJSON)
	if len(dockerConfig.Auths) != 3 {
		t.Fatalf("Docker auth entries = %d, want 3", len(dockerConfig.Auths))
	}
	authA := dockerConfig.Auths["a.example.com"]
	if authA != dockerConfig.Auths["a-vpc.example.com"] {
		t.Fatal("domains for one Registry do not share one credential")
	}
	if authA.Auth != encodedAuth("user-a", "password-a") {
		t.Fatalf("Docker auth = %q, want base64(username:password)", authA.Auth)
	}

	state := decodeState(t, first.StateJSON)
	if state.Version != StateVersion || len(state.Registries) != 2 {
		t.Fatalf("state = %+v, want version %d and two Registries", state, StateVersion)
	}
	stateA := state.Registries[registryA.Key().String()]
	if stateA.StateHash == "" || stateA.RefreshedAt.Location() != time.UTC {
		t.Fatalf("Registry A state = %+v, want hash and UTC timestamps", stateA)
	}
}

func TestStateHashIsDeterministicAndCoversCredentialState(t *testing.T) {
	t.Parallel()

	registry := testRegistry("cn-hangzhou", "cri-a", "key", "secret", "a.example.com")
	value := testCredential("user", "password", time.Date(2026, time.August, 29, 10, 0, 0, 0, time.UTC))
	baseline := stateHash(registry, value)
	if baseline != stateHash(registry, value) {
		t.Fatal("stateHash() is not deterministic")
	}

	changed := value
	changed.Password = "new-password"
	if baseline == stateHash(registry, changed) {
		t.Fatal("stateHash() ignored credential password")
	}
	changed = value
	changed.ExpiresAt = changed.ExpiresAt.Add(time.Second)
	if baseline == stateHash(registry, changed) {
		t.Fatal("stateHash() ignored expiration")
	}
	changedRegistry := registry
	changedRegistry.AccessKeySecret = "new-secret"
	if baseline == stateHash(changedRegistry, value) {
		t.Fatal("stateHash() ignored Registry configuration")
	}
}

func TestBuildPreservesPendingRegistryWhileUpdatingOthers(t *testing.T) {
	t.Parallel()

	registryA := testRegistry("cn-hangzhou", "cri-a", "key-a", "secret-a", "a.example.com")
	registryB := testRegistry("cn-shanghai", "cri-b", "key-b", "secret-b", "b.example.com")
	now := time.Date(2026, time.August, 29, 10, 0, 0, 0, time.UTC)
	initialCredentials := map[config.RegistryKey]credential.Entry{
		registryA.Key(): testEntry(registryA, testCredential("user-a", "old-a", now)),
		registryB.Key(): testEntry(registryB, testCredential("user-b", "old-b", now)),
	}
	initial, err := Build(testSnapshot(registryA, registryB), initialCredentials, nil)
	if err != nil {
		t.Fatalf("initial Build() error = %v", err)
	}
	existing := secretFromContent("production", initial)

	updated, err := Build(
		testSnapshot(registryA, registryB),
		map[config.RegistryKey]credential.Entry{
			registryA.Key(): testEntry(registryA, testCredential("user-a", "new-a", now.Add(time.Minute))),
		},
		existing,
	)
	if err != nil {
		t.Fatalf("pending Build() error = %v", err)
	}
	dockerConfig := decodeDockerConfig(t, updated.DockerJSON)
	if dockerConfig.Auths["a.example.com"].Password != "new-a" {
		t.Fatal("available Registry did not update")
	}
	if dockerConfig.Auths["b.example.com"].Password != "old-b" {
		t.Fatal("pending Registry old Auth was not preserved")
	}
	updatedState := decodeState(t, updated.StateJSON)
	initialState := decodeState(t, initial.StateJSON)
	if updatedState.Registries[registryB.Key().String()] != initialState.Registries[registryB.Key().String()] {
		t.Fatal("pending Registry old State was not preserved")
	}

	withoutB, err := Build(
		testSnapshot(registryA),
		map[config.RegistryKey]credential.Entry{
			registryA.Key(): testEntry(registryA, testCredential("user-a", "new-a", now.Add(time.Minute))),
		},
		secretFromContent("production", updated),
	)
	if err != nil {
		t.Fatalf("Build() after Registry deletion error = %v", err)
	}
	if _, found := decodeDockerConfig(t, withoutB.DockerJSON).Auths["b.example.com"]; found {
		t.Fatal("deleted Registry Auth was preserved")
	}
	if _, found := decodeState(t, withoutB.StateJSON).Registries[registryB.Key().String()]; found {
		t.Fatal("deleted Registry State was preserved")
	}
}

func TestBuildRejectsIncompletePendingState(t *testing.T) {
	t.Parallel()

	registry := testRegistry("cn-hangzhou", "cri-a", "key", "secret", "a.example.com")
	existing := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{StateAnnotationKey: `{"version":1,"registries":{}}`}},
		Data:       map[string][]byte{corev1.DockerConfigJsonKey: []byte(`{"auths":{"a.example.com":{"username":"user","password":"password","auth":"invalid"}}}`)},
	}
	if _, err := Build(testSnapshot(registry), nil, existing); err == nil {
		t.Fatal("Build() accepted pending Docker Auth without matching State")
	}
}

func TestBuildDoesNotRehashOldCredentialAfterAccessKeyRotation(t *testing.T) {
	t.Parallel()

	oldRegistry := testRegistry("cn-hangzhou", "cri-a", "old-key", "old-secret", "a.example.com")
	value := testCredential("user", "old-password", time.Date(2026, time.August, 29, 10, 0, 0, 0, time.UTC))
	oldEntry := testEntry(oldRegistry, value)
	initial, err := Build(
		testSnapshot(oldRegistry),
		map[config.RegistryKey]credential.Entry{oldRegistry.Key(): oldEntry},
		nil,
	)
	if err != nil {
		t.Fatalf("initial Build() error = %v", err)
	}

	rotatedRegistry := oldRegistry
	rotatedRegistry.AccessKeyID = "new-key"
	rotatedRegistry.AccessKeySecret = "new-secret"
	rotated, err := Build(
		testSnapshot(rotatedRegistry),
		map[config.RegistryKey]credential.Entry{oldRegistry.Key(): oldEntry},
		secretFromContent("production", initial),
	)
	if err != nil {
		t.Fatalf("rotated Build() error = %v", err)
	}
	if rotated.StateJSON != initial.StateJSON || string(rotated.DockerJSON) != string(initial.DockerJSON) {
		t.Fatal("old credential was rehashed as if it came from the rotated AK/SK")
	}
}

func testSnapshot(registries ...config.RegistryConfig) config.ConfigurationSnapshot {
	snapshot := config.ConfigurationSnapshot{
		Namespaces:      config.NameSelector{MatchAll: true},
		ServiceAccounts: config.NameSelector{MatchAll: true},
		Registries:      make(map[config.RegistryKey]config.RegistryConfig, len(registries)),
	}
	for _, registry := range registries {
		snapshot.Registries[registry.Key()] = registry
	}
	return snapshot
}

func testRegistry(region, instance, accessKeyID, accessKeySecret string, domains ...string) config.RegistryConfig {
	return config.RegistryConfig{
		RegionID:        region,
		InstanceID:      instance,
		AccessKeyID:     accessKeyID,
		AccessKeySecret: accessKeySecret,
		Domains:         append([]string(nil), domains...),
	}
}

func testCredential(username, password string, refreshedAt time.Time) credential.Credential {
	return credential.Credential{
		Username:    username,
		Password:    password,
		RefreshedAt: refreshedAt,
		ExpiresAt:   refreshedAt.Add(time.Hour),
	}
}

func testEntry(registry config.RegistryConfig, value credential.Credential) credential.Entry {
	store := &credential.Store{}
	return store.Apply(registry, value)
}

func decodeDockerConfig(t *testing.T, data []byte) DockerConfig {
	t.Helper()
	var dockerConfig DockerConfig
	if err := json.Unmarshal(data, &dockerConfig); err != nil {
		t.Fatalf("unmarshal Docker config: %v", err)
	}
	return dockerConfig
}

func decodeState(t *testing.T, raw string) PersistedState {
	t.Helper()
	var state PersistedState
	if err := json.Unmarshal([]byte(raw), &state); err != nil {
		t.Fatalf("unmarshal State: %v", err)
	}
	return state
}

func secretFromContent(namespace string, content Content) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "auto-patch-secret",
			Namespace:   namespace,
			Labels:      map[string]string{ApplicationNameLabelKey: ControllerIdentity, ManagedByLabelKey: ControllerIdentity},
			Annotations: map[string]string{StateAnnotationKey: content.StateJSON},
		},
		Type: corev1.SecretTypeDockerConfigJson,
		Data: map[string][]byte{corev1.DockerConfigJsonKey: append([]byte(nil), content.DockerJSON...)},
	}
}
