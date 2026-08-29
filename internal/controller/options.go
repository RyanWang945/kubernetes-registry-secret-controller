package controller

import (
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	DefaultControllerNamespace                   = "registry-secret-controller-system"
	DefaultConfigMapName                         = "registry-secret-controller-config"
	DefaultManagedSecretName                     = "auto-patch-secret"
	DefaultMaxConcurrentNamespaceReconciles      = 2
	DefaultMaxConcurrentServiceAccountReconciles = 2

	DefaultLeaderElectionID = "kubernetes-registry-secret-controller"

	controllerEventBuffer = 1024
)

// ControllerOptions contains the fixed Kubernetes object identities and the
// bounded concurrency used by the controllers registered with one Manager.
type ControllerOptions struct {
	// ControllerNamespace is the namespace containing the controller's fixed
	// configuration ConfigMap. It does not determine the namespace in which the
	// controller Pod runs or restrict which namespaces the controller manages.
	ControllerNamespace string

	// ConfigMapName is the name of the configuration ConfigMap read from
	// ControllerNamespace.
	ConfigMapName string

	// ManagedSecretName is the name of the registry Secret created and maintained
	// in each target namespace.
	ManagedSecretName string

	// MaxConcurrentNamespaceReconciles limits how many namespace Secret
	// reconciliation requests may run concurrently; it does not limit the number
	// of managed namespaces.
	MaxConcurrentNamespaceReconciles int

	// MaxConcurrentServiceAccountReconciles limits how many individual
	// ServiceAccounts may be reconciled concurrently.
	MaxConcurrentServiceAccountReconciles int
}

func (o ControllerOptions) withDefaults() ControllerOptions {
	if o.ControllerNamespace == "" {
		o.ControllerNamespace = DefaultControllerNamespace
	}
	if o.ConfigMapName == "" {
		o.ConfigMapName = DefaultConfigMapName
	}
	if o.ManagedSecretName == "" {
		o.ManagedSecretName = DefaultManagedSecretName
	}
	if o.MaxConcurrentNamespaceReconciles == 0 {
		o.MaxConcurrentNamespaceReconciles = DefaultMaxConcurrentNamespaceReconciles
	}
	if o.MaxConcurrentServiceAccountReconciles == 0 {
		o.MaxConcurrentServiceAccountReconciles = DefaultMaxConcurrentServiceAccountReconciles
	}
	return o
}

func (o ControllerOptions) validate() error {
	if problems := validation.IsDNS1123Label(o.ControllerNamespace); len(problems) > 0 {
		return fmt.Errorf("invalid controller namespace: %s", strings.Join(problems, "; "))
	}
	if problems := validation.IsDNS1123Subdomain(o.ConfigMapName); len(problems) > 0 {
		return fmt.Errorf("invalid ConfigMap name: %s", strings.Join(problems, "; "))
	}
	if problems := validation.IsDNS1123Subdomain(o.ManagedSecretName); len(problems) > 0 {
		return fmt.Errorf("invalid managed Secret name: %s", strings.Join(problems, "; "))
	}
	if o.MaxConcurrentNamespaceReconciles < 1 {
		return fmt.Errorf(
			"maximum concurrent namespace reconciles must be at least one, got %d",
			o.MaxConcurrentNamespaceReconciles,
		)
	}
	if o.MaxConcurrentServiceAccountReconciles < 1 {
		return fmt.Errorf(
			"maximum concurrent ServiceAccount reconciles must be at least one, got %d",
			o.MaxConcurrentServiceAccountReconciles,
		)
	}
	return nil
}

// NewCacheOptions scopes every cached object type deliberately. In particular,
// the Secret cache includes every Secret with the fixed output name, including
// unowned collisions that the reconciler must observe and reject.
func NewCacheOptions(options ControllerOptions) (cache.Options, error) {
	options = options.withDefaults()
	if err := options.validate(); err != nil {
		return cache.Options{}, err
	}

	configMapName := fields.OneTermEqualSelector("metadata.name", options.ConfigMapName)
	managedSecretName := fields.OneTermEqualSelector("metadata.name", options.ManagedSecretName)

	return cache.Options{
		ReaderFailOnMissingInformer: true,
		DefaultTransform:            cache.TransformStripManagedFields(),
		ByObject: map[client.Object]cache.ByObject{
			&corev1.ConfigMap{}: {
				Namespaces: map[string]cache.Config{
					options.ControllerNamespace: {},
				},
				Field: configMapName,
			},
			&corev1.Namespace{}:      {},
			&corev1.ServiceAccount{}: {},
			&corev1.Secret{}: {
				Field: managedSecretName,
			},
		},
	}, nil
}
