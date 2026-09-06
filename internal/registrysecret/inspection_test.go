package registrysecret

import (
	"encoding/json"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/RyanWang945/kubernetes-registry-secret-controller/internal/config"
	"github.com/RyanWang945/kubernetes-registry-secret-controller/internal/credential"
)

func TestInspectionIsolatesRegistryDamageAndPendingDomainChanges(t *testing.T) {
	now := time.Now().UTC()
	a := testRegistry("cn-test", "cri-a", "key-a", "secret-a", "a.example.com")
	b := testRegistry("cn-test", "cri-b", "key-b", "secret-b", "b.example.com")
	creds := &credential.Store{}
	for _, r := range []config.RegistryConfig{a, b} {
		creds.Apply(r, testCredential("username", "password", now))
	}
	content, err := Build(testSnapshot(a, b), creds.Snapshot(), nil)
	if err != nil {
		t.Fatal(err)
	}
	secret := newSecret(client.ObjectKey{Namespace: "target", Name: "auto-patch-secret"}, content)
	expected := Inspect(secret.DeepCopy())
	var envelope struct {
		Version    int                        `json:"version"`
		Registries map[string]json.RawMessage `json:"registries"`
	}
	if err := json.Unmarshal([]byte(content.StateJSON), &envelope); err != nil {
		t.Fatal(err)
	}
	envelope.Registries[b.Key().String()] = json.RawMessage(`{"refreshedAt":"malformed"}`)
	data, err := json.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	secret.Annotations[StateAnnotationKey] = string(data)
	actual := Inspect(secret)
	if state, _ := actual.RegistryStatus(a, expected, now); state != "synced" {
		t.Fatalf("damage in B changed A's state to %s", state)
	}
	if state, expiry := actual.RegistryStatus(b, expected, now); state != "invalid" || !expiry.IsZero() {
		t.Fatalf("damaged B = %s, expiry=%v", state, expiry)
	}
	a.Domains = append(a.Domains, "new.example.com")
	if state, expiry := actual.RegistryStatus(a, expected, now); state != "pending" || expiry.IsZero() {
		t.Fatalf("new domain hid the usable old copy: %s, expiry=%v", state, expiry)
	}
	entry, _ := creds.Load(a.Key())
	if state, _ := actual.RegistryStatus(a, expected, entry.Credential.ExpiresAt); state != "expired" {
		t.Fatalf("domain addition hid expiry: %s", state)
	}
	secret.Data[corev1.DockerConfigJsonKey] = []byte(`{"auths":{}}`)
	if state, expiry := Inspect(secret).RegistryStatus(a, expected, now); state != "pending" || !expiry.IsZero() {
		t.Fatalf("missing auth reported an expiry: %s, expiry=%v", state, expiry)
	}
}
