package controller

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/priorityqueue"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

func TestServiceAccountEventHandlerPrioritizesLiveCreate(t *testing.T) {
	t.Parallel()

	queue := priorityqueue.New[reconcile.Request](t.Name())
	t.Cleanup(queue.ShutDown)
	subject := &serviceAccountEventHandler{}
	ctx := context.Background()
	for _, namespace := range []string{"production", "staging"} {
		subject.Generic(ctx, event.GenericEvent{Object: priorityTestServiceAccount(namespace, "default")}, queue)
	}
	created := priorityTestServiceAccount("production", "new-live")
	subject.Create(ctx, event.CreateEvent{Object: created}, queue)

	request, priority, shutdown := queue.GetWithPriority()
	if shutdown {
		t.Fatal("queue shut down before returning the new ServiceAccount")
	}
	queue.Done(request)
	if request.NamespacedName != client.ObjectKeyFromObject(created) || priority != liveServiceAccountCreatePriority {
		t.Fatalf("first request = %s at priority %d, want %s at priority %d", request.NamespacedName, priority, client.ObjectKeyFromObject(created), liveServiceAccountCreatePriority)
	}
	if queue.Len() != 2 {
		t.Fatalf("remaining queue length = %d, want two independent namespace/name keys", queue.Len())
	}
}

func TestServiceAccountEventHandlerPreservesOrdinaryEventPriorities(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name     string
		priority int
		enqueue  func(*serviceAccountEventHandler, *corev1.ServiceAccount, priorityqueue.PriorityQueue[reconcile.Request])
	}{
		{
			name: "initial-list", priority: handler.LowPriority,
			enqueue: func(h *serviceAccountEventHandler, sa *corev1.ServiceAccount, q priorityqueue.PriorityQueue[reconcile.Request]) {
				h.Create(context.Background(), event.CreateEvent{Object: sa, IsInInitialList: true}, q)
			},
		},
		{
			name: "update",
			enqueue: func(h *serviceAccountEventHandler, sa *corev1.ServiceAccount, q priorityqueue.PriorityQueue[reconcile.Request]) {
				updated := sa.DeepCopy()
				updated.ResourceVersion = "2"
				h.Update(context.Background(), event.UpdateEvent{ObjectOld: sa, ObjectNew: updated}, q)
			},
		},
		{
			name: "resync", priority: handler.LowPriority,
			enqueue: func(h *serviceAccountEventHandler, sa *corev1.ServiceAccount, q priorityqueue.PriorityQueue[reconcile.Request]) {
				h.Update(context.Background(), event.UpdateEvent{ObjectOld: sa, ObjectNew: sa.DeepCopy()}, q)
			},
		},
		{
			name: "delete",
			enqueue: func(h *serviceAccountEventHandler, sa *corev1.ServiceAccount, q priorityqueue.PriorityQueue[reconcile.Request]) {
				h.Delete(context.Background(), event.DeleteEvent{Object: sa}, q)
			},
		},
		{
			name: "configuration-fanout",
			enqueue: func(h *serviceAccountEventHandler, sa *corev1.ServiceAccount, q priorityqueue.PriorityQueue[reconcile.Request]) {
				h.Generic(context.Background(), event.GenericEvent{Object: sa}, q)
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			queue := priorityqueue.New[reconcile.Request](t.Name())
			t.Cleanup(queue.ShutDown)
			sa := priorityTestServiceAccount("production", "default")
			test.enqueue(&serviceAccountEventHandler{}, sa, queue)
			request, priority, shutdown := queue.GetWithPriority()
			if shutdown {
				t.Fatal("queue shut down before returning the ServiceAccount")
			}
			queue.Done(request)
			if request.NamespacedName != client.ObjectKeyFromObject(sa) || priority != test.priority {
				t.Fatalf("request = %s at priority %d, want %s at priority %d", request.NamespacedName, priority, client.ObjectKeyFromObject(sa), test.priority)
			}
		})
	}
}

func TestServiceAccountEventHandlerPromotesAndDeduplicatesQueuedCreate(t *testing.T) {
	t.Parallel()

	queue := priorityqueue.New[reconcile.Request](t.Name())
	t.Cleanup(queue.ShutDown)
	sa := priorityTestServiceAccount("production", "default")
	subject := &serviceAccountEventHandler{}
	ctx := context.Background()
	subject.Generic(ctx, event.GenericEvent{Object: sa}, queue)
	subject.Create(ctx, event.CreateEvent{Object: sa}, queue)
	// A later ordinary event must not downgrade or duplicate the urgent item.
	subject.Generic(ctx, event.GenericEvent{Object: sa}, queue)

	request, priority, shutdown := queue.GetWithPriority()
	if shutdown {
		t.Fatal("queue shut down before returning the promoted ServiceAccount")
	}
	queue.Done(request)
	if request.NamespacedName != client.ObjectKeyFromObject(sa) || priority != liveServiceAccountCreatePriority {
		t.Fatalf("request = %s at priority %d, want promoted %s", request.NamespacedName, priority, client.ObjectKeyFromObject(sa))
	}
	if queue.Len() != 0 {
		t.Fatalf("queue length after de-duplicated request = %d, want 0", queue.Len())
	}
}

func TestServiceAccountEventHandlerFallsBackWithoutPriorityQueue(t *testing.T) {
	t.Parallel()

	queue := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[reconcile.Request]())
	t.Cleanup(queue.ShutDown)
	sa := priorityTestServiceAccount("production", "default")
	(&serviceAccountEventHandler{}).Create(context.Background(), event.CreateEvent{Object: sa}, queue)
	request, shutdown := queue.Get()
	if shutdown {
		t.Fatal("queue shut down before returning the fallback request")
	}
	queue.Done(request)
	if request.NamespacedName != client.ObjectKeyFromObject(sa) {
		t.Fatalf("fallback request = %s, want %s", request.NamespacedName, client.ObjectKeyFromObject(sa))
	}
}

func priorityTestServiceAccount(namespace, name string) *corev1.ServiceAccount {
	return &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name, ResourceVersion: "1"}}
}
