package serviceaccount

import (
	"context"
	"errors"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/RyanWang945/kubernetes-registry-secret-controller/internal/config"
	"github.com/RyanWang945/kubernetes-registry-secret-controller/internal/registrysecret"
)

type Syncer struct {
	client      client.Client
	configStore *config.Store
	secretName  string
}

func NewSyncer(kubernetesClient client.Client, configStore *config.Store, secretName string) (*Syncer, error) {
	if kubernetesClient == nil {
		return nil, errors.New("Kubernetes client must not be nil")
	}
	if configStore == nil {
		return nil, errors.New("config store must not be nil")
	}
	if secretName == "" {
		return nil, errors.New("managed Secret name must not be empty")
	}
	return &Syncer{client: kubernetesClient, configStore: configStore, secretName: secretName}, nil
}

func (s *Syncer) SyncServiceAccount(ctx context.Context, key types.NamespacedName) error {
	snapshot, loaded := s.configStore.Load()
	if !loaded {
		return nil
	}

	serviceAccount := &corev1.ServiceAccount{}
	if err := s.client.Get(ctx, key, serviceAccount); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("get ServiceAccount %s: %w", key, err)
	}

	shouldReference := snapshot.MatchesNamespace(key.Namespace) && snapshot.ServiceAccounts.Matches(key.Name)
	if shouldReference {
		secret := &corev1.Secret{}
		secretKey := client.ObjectKey{Namespace: key.Namespace, Name: s.secretName}
		if err := s.client.Get(ctx, secretKey, secret); err != nil {
			if apierrors.IsNotFound(err) {
				return fmt.Errorf("managed Secret %s is not ready", secretKey)
			}
			return fmt.Errorf("get managed Secret %s: %w", secretKey, err)
		}
		if !registrysecret.IsManaged(secret) {
			conflict := fmt.Errorf("Secret %s exists but is not owned by this controller", secretKey)
			log.FromContext(ctx).Error(conflict, "cannot inject registry Secret reference")
			shouldReference = false
		}
	}

	desiredReferences := reconcileReferences(serviceAccount.ImagePullSecrets, s.secretName, shouldReference)
	if equalReferences(serviceAccount.ImagePullSecrets, desiredReferences) {
		return nil
	}
	base := serviceAccount.DeepCopy()
	serviceAccount.ImagePullSecrets = desiredReferences
	patch := client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{})
	if err := s.client.Patch(ctx, serviceAccount, patch); err != nil {
		return fmt.Errorf("patch ServiceAccount %s imagePullSecrets: %w", key, err)
	}
	return nil
}

func reconcileReferences(
	current []corev1.LocalObjectReference,
	managedName string,
	shouldReference bool,
) []corev1.LocalObjectReference {
	result := make([]corev1.LocalObjectReference, 0, len(current)+1)
	managedAdded := false
	for _, reference := range current {
		if reference.Name != managedName {
			result = append(result, reference)
			continue
		}
		if shouldReference && !managedAdded {
			result = append(result, corev1.LocalObjectReference{Name: managedName})
			managedAdded = true
		}
	}
	if shouldReference && !managedAdded {
		result = append(result, corev1.LocalObjectReference{Name: managedName})
	}
	return result
}

func equalReferences(left, right []corev1.LocalObjectReference) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index].Name != right[index].Name {
			return false
		}
	}
	return true
}
