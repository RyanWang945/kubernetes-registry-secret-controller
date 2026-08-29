package controller

import (
	"context"
	"errors"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
)

// ConfigurationObserver is notified after a valid configuration has been
// installed. Implementations must return quickly; the configuration
// controller must not wait for downstream work to finish.
type ConfigurationObserver interface {
	NotifyConfigurationChanged()
}

// ResourceEventPublisher owns the in-process event channels shared by the
// configuration controller, the credential scheduler and the resource
// controllers. Publishing a Namespace event schedules Secret convergence; it
// does not perform the convergence inline.
type ResourceEventPublisher struct {
	reader               client.Reader
	namespaceEvents      chan event.GenericEvent
	serviceAccountEvents chan event.GenericEvent
}

func NewResourceEventPublisher(reader client.Reader) (*ResourceEventPublisher, error) {
	if reader == nil {
		return nil, errors.New("resource event reader must not be nil")
	}
	return &ResourceEventPublisher{
		reader:               reader,
		namespaceEvents:      make(chan event.GenericEvent, controllerEventBuffer),
		serviceAccountEvents: make(chan event.GenericEvent, controllerEventBuffer),
	}, nil
}

// PublishAllNamespaces puts every Namespace on the Namespace Secret
// controller's queue. The controller applies the latest configuration gate
// when it processes each event.
func (p *ResourceEventPublisher) PublishAllNamespaces(ctx context.Context) error {
	namespaces := &corev1.NamespaceList{}
	if err := p.reader.List(ctx, namespaces); err != nil {
		return fmt.Errorf("list Namespaces for event fan-out: %w", err)
	}

	for i := range namespaces.Items {
		namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespaces.Items[i].Name}}
		select {
		case p.namespaceEvents <- event.GenericEvent{Object: namespace}:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// PublishAllServiceAccounts puts every ServiceAccount on the object-level
// controller queue. Filtering and cleanup decisions use the latest snapshot
// during reconciliation.
func (p *ResourceEventPublisher) PublishAllServiceAccounts(ctx context.Context) error {
	serviceAccounts := &corev1.ServiceAccountList{}
	if err := p.reader.List(ctx, serviceAccounts); err != nil {
		return fmt.Errorf("list ServiceAccounts for event fan-out: %w", err)
	}

	for i := range serviceAccounts.Items {
		serviceAccount := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{
			Namespace: serviceAccounts.Items[i].Namespace,
			Name:      serviceAccounts.Items[i].Name,
		}}
		select {
		case p.serviceAccountEvents <- event.GenericEvent{Object: serviceAccount}:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}
