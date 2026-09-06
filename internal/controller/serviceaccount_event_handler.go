package controller

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/priorityqueue"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// liveServiceAccountCreatePriority keeps new ServiceAccounts ahead of pending
// configuration fan-out. Initial-list and ordinary events retain the default
// handler's priorities; Secret readiness and client rate limits still apply.
const liveServiceAccountCreatePriority = 100

type serviceAccountEventHandler struct {
	handler.EnqueueRequestForObject
}

var _ handler.EventHandler = &serviceAccountEventHandler{}

func (h *serviceAccountEventHandler) Create(
	ctx context.Context,
	evt event.CreateEvent,
	queue workqueue.TypedRateLimitingInterface[reconcile.Request],
) {
	serviceAccount, ok := evt.Object.(*corev1.ServiceAccount)
	if evt.IsInInitialList || !ok || serviceAccount == nil {
		h.EnqueueRequestForObject.Create(ctx, evt, queue)
		return
	}

	request := reconcile.Request{NamespacedName: client.ObjectKeyFromObject(serviceAccount)}
	if priorityQueue, ok := queue.(priorityqueue.PriorityQueue[reconcile.Request]); ok {
		priorityQueue.AddWithOpts(priorityqueue.AddOpts{
			Priority: ptr.To(liveServiceAccountCreatePriority),
		}, request)
		return
	}
	queue.Add(request)
}
