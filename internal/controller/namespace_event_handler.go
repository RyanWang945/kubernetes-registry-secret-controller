package controller

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/controller/priorityqueue"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// liveNamespaceCreatePriority keeps newly-created namespaces responsive while
// a credential refresh has enqueued a large namespace fan-out. The default
// priority is zero and controller-runtime uses -100 for initial-list events.
const liveNamespaceCreatePriority = 100

type namespaceEventHandler struct {
	defaultHandler handler.EnqueueRequestForObject
}

var _ handler.EventHandler = &namespaceEventHandler{}

func (h *namespaceEventHandler) Create(
	ctx context.Context,
	evt event.CreateEvent,
	queue workqueue.TypedRateLimitingInterface[reconcile.Request],
) {
	// Preserve controller-runtime's low-priority treatment for the initial
	// informer list. Only a create observed after the watch is live is urgent.
	namespace, ok := evt.Object.(*corev1.Namespace)
	if evt.IsInInitialList || !ok || namespace == nil {
		h.defaultHandler.Create(ctx, evt, queue)
		return
	}

	request := reconcile.Request{NamespacedName: types.NamespacedName{
		Name: namespace.Name,
	}}
	if priorityQueue, ok := queue.(priorityqueue.PriorityQueue[reconcile.Request]); ok {
		priorityQueue.AddWithOpts(priorityqueue.AddOpts{
			Priority: ptr.To(liveNamespaceCreatePriority),
		}, request)
		return
	}

	// Keep the handler usable if priority queues are explicitly disabled in a
	// future Manager configuration.
	queue.Add(request)
}

func (h *namespaceEventHandler) Update(
	ctx context.Context,
	evt event.UpdateEvent,
	queue workqueue.TypedRateLimitingInterface[reconcile.Request],
) {
	h.defaultHandler.Update(ctx, evt, queue)
}

func (h *namespaceEventHandler) Delete(
	ctx context.Context,
	evt event.DeleteEvent,
	queue workqueue.TypedRateLimitingInterface[reconcile.Request],
) {
	h.defaultHandler.Delete(ctx, evt, queue)
}

func (h *namespaceEventHandler) Generic(
	ctx context.Context,
	evt event.GenericEvent,
	queue workqueue.TypedRateLimitingInterface[reconcile.Request],
) {
	h.defaultHandler.Generic(ctx, evt, queue)
}
