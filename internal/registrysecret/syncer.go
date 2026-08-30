package registrysecret

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/RyanWang945/kubernetes-registry-secret-controller/internal/config"
	"github.com/RyanWang945/kubernetes-registry-secret-controller/internal/credential"
)

type Syncer struct {
	client          client.Client
	configStore     *config.Store
	credentialStore *credential.Store
	secretName      string
	eventRecorder   record.EventRecorder
}

func NewSyncer(
	kubernetesClient client.Client,
	configStore *config.Store,
	credentialStore *credential.Store,
	secretName string,
	eventRecorder record.EventRecorder,
) (*Syncer, error) {
	if kubernetesClient == nil {
		return nil, errors.New("Kubernetes client must not be nil")
	}
	if configStore == nil {
		return nil, errors.New("config store must not be nil")
	}
	if credentialStore == nil {
		return nil, errors.New("credential store must not be nil")
	}
	if secretName == "" {
		return nil, errors.New("managed Secret name must not be empty")
	}
	if eventRecorder == nil {
		return nil, errors.New("event recorder must not be nil")
	}
	return &Syncer{
		client:          kubernetesClient,
		configStore:     configStore,
		credentialStore: credentialStore,
		secretName:      secretName,
		eventRecorder:   eventRecorder,
	}, nil
}

func (s *Syncer) SyncNamespaceSecret(ctx context.Context, namespace string) error {
	snapshot, loaded := s.configStore.Load()
	if !loaded {
		return nil
	}

	namespaceObject := &corev1.Namespace{}
	if err := s.client.Get(ctx, client.ObjectKey{Name: namespace}, namespaceObject); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("get Namespace %q: %w", namespace, err)
	}

	key := client.ObjectKey{Namespace: namespace, Name: s.secretName}
	current := &corev1.Secret{}
	err := s.client.Get(ctx, key, current)
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("get managed Secret %s: %w", key, err)
	}

	if !snapshot.MatchesNamespace(namespace) {
		if apierrors.IsNotFound(err) || !IsManaged(current) {
			return nil
		}
		if err := s.client.Delete(ctx, current); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("delete managed Secret %s: %w", key, err)
		}
		return nil
	}

	if err == nil && !IsManaged(current) {
		conflict := fmt.Errorf("Secret %s exists but is not owned by this controller", key)
		log.FromContext(ctx).Error(conflict, "cannot manage registry Secret")
		s.eventRecorder.Eventf(
			current,
			corev1.EventTypeWarning,
			"OwnershipConflict",
			"Secret %s already exists and is not managed by %s",
			key,
			ControllerIdentity,
		)
		return nil
	}

	var existing *corev1.Secret
	if err == nil {
		existing = current
	}
	content, buildErr := Build(snapshot, s.credentialStore.Snapshot(), existing)
	if buildErr != nil {
		return fmt.Errorf("build managed Secret %s: %w", key, buildErr)
	}
	if !content.HasAuth() {
		if apierrors.IsNotFound(err) {
			return nil
		}
		if err := s.client.Delete(ctx, current); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("delete empty managed Secret %s: %w", key, err)
		}
		return nil
	}

	if apierrors.IsNotFound(err) {
		desired := newSecret(key, content)
		if err := s.client.Create(ctx, desired); err != nil {
			return fmt.Errorf("create managed Secret %s: %w", key, err)
		}
		return nil
	}

	if secretMatches(current, content) {
		return nil
	}
	applyContent(current, content)
	if err := s.client.Update(ctx, current); err != nil {
		return fmt.Errorf("update managed Secret %s: %w", key, err)
	}
	return nil
}

func IsManaged(secret *corev1.Secret) bool {
	return secret != nil &&
		secret.Labels[ApplicationNameLabelKey] == ControllerIdentity &&
		secret.Labels[ManagedByLabelKey] == ControllerIdentity
}

// IsUsableForServiceAccount performs the small, cache-friendly validation
// needed before a ServiceAccount first references the fixed Secret name.
// Namespace Secret reconciliation remains responsible for full content
// correctness and drift repair.
func IsUsableForServiceAccount(secret *corev1.Secret) bool {
	return IsManaged(secret) &&
		secret.DeletionTimestamp == nil &&
		secret.Type == corev1.SecretTypeDockerConfigJson &&
		len(secret.Data[corev1.DockerConfigJsonKey]) > 0
}

func newSecret(key client.ObjectKey, content Content) *corev1.Secret {
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{
		Namespace: key.Namespace,
		Name:      key.Name,
	}}
	applyContent(secret, content)
	return secret
}

func applyContent(secret *corev1.Secret, content Content) {
	secret.Type = corev1.SecretTypeDockerConfigJson
	secret.Data = map[string][]byte{corev1.DockerConfigJsonKey: append([]byte(nil), content.DockerJSON...)}
	secret.Labels = cloneStringMap(secret.Labels)
	secret.Labels[ApplicationNameLabelKey] = ControllerIdentity
	secret.Labels[ManagedByLabelKey] = ControllerIdentity
	secret.Annotations = cloneStringMap(secret.Annotations)
	secret.Annotations[StateAnnotationKey] = content.StateJSON
}

func secretMatches(secret *corev1.Secret, content Content) bool {
	return secret.Type == corev1.SecretTypeDockerConfigJson &&
		len(secret.Data) == 1 &&
		bytes.Equal(secret.Data[corev1.DockerConfigJsonKey], content.DockerJSON) &&
		secret.Labels[ApplicationNameLabelKey] == ControllerIdentity &&
		secret.Labels[ManagedByLabelKey] == ControllerIdentity &&
		secret.Annotations[StateAnnotationKey] == content.StateJSON
}

func cloneStringMap(source map[string]string) map[string]string {
	clone := make(map[string]string, len(source)+2)
	for key, value := range source {
		clone[key] = value
	}
	return clone
}
