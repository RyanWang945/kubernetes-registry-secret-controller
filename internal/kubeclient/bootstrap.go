package kubeclient

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/RyanWang945/kubernetes-registry-secret-controller/internal/config"
)

// Bootstrap reads the fixed ConfigMap once before constructing workers. It does
// not populate Config Store or mark readiness: the synchronized configuration
// controller still owns that. API failures abort startup instead of silently
// fixing workers at the wrong count. Missing/invalid config uses startup values.
func Bootstrap(ctx context.Context, cfg *rest.Config, key client.ObjectKey, fallback Settings) (*Runtime, error) {
	api, err := typedcorev1.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("create bootstrap configuration client: %w", err)
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	cm, err := api.ConfigMaps(key.Namespace).Get(ctx, key.Name, metav1.GetOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return nil, fmt.Errorf("read startup ConfigMap: %w", err)
	}
	if apierrors.IsNotFound(err) {
		cm = nil
	}
	return runtimeFromConfigMap(ctx, cm, key, fallback)
}

func runtimeFromConfigMap(ctx context.Context, cm *corev1.ConfigMap, key client.ObjectKey, fallback Settings) (*Runtime, error) {
	var overrides config.RuntimeConfiguration
	if cm != nil {
		snapshot, err := config.Parse(cm.Data)
		if err == nil {
			overrides = snapshot.Runtime
		} else {
			// Never log the parser's input-bearing error. The normal reconciler
			// will also report InvalidConfiguration once its cache is synced.
			log.FromContext(ctx).Info("startup configuration is invalid; using startup tuning values", "namespace", key.Namespace, "name", key.Name, "reason", "InvalidConfiguration")
		}
	}
	return NewRuntime(fallback, overrides)
}
