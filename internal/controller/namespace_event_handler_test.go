package controller

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/controller/priorityqueue"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

func TestNamespaceEventHandlerPrioritizesLiveCreate(t *testing.T) {
	t.Parallel()

	queue := priorityqueue.New[reconcile.Request](t.Name())
	t.Cleanup(queue.ShutDown)
	defaultHandler := &handler.EnqueueRequestForObject{}
	for _, name := range []string{"refresh-1", "refresh-2"} {
		defaultHandler.Generic(context.Background(), event.GenericEvent{
			Object: testNamespace(name),
		}, queue)
	}

	subject := &namespaceEventHandler{}
	subject.Create(context.Background(), event.CreateEvent{
		Object: testNamespace("new-live"),
	}, queue)

	request, priority, shutdown := queue.GetWithPriority()
	if shutdown {
		t.Fatal("queue shut down before returning the live Namespace")
	}
	queue.Done(request)
	if request.Name != "new-live" || request.Namespace != "" {
		t.Fatalf("first request = %q, want cluster-scoped new-live", request.NamespacedName)
	}
	if priority != liveNamespaceCreatePriority {
		t.Fatalf("live Namespace priority = %d, want %d", priority, liveNamespaceCreatePriority)
	}
}

func TestNamespaceEventHandlerPreservesInitialListPriority(t *testing.T) {
	t.Parallel()

	queue := priorityqueue.New[reconcile.Request](t.Name())
	t.Cleanup(queue.ShutDown)
	subject := &namespaceEventHandler{}
	subject.Create(context.Background(), event.CreateEvent{
		Object:          testNamespace("initial"),
		IsInInitialList: true,
	}, queue)

	request, priority, shutdown := queue.GetWithPriority()
	if shutdown {
		t.Fatal("queue shut down before returning the initial-list Namespace")
	}
	queue.Done(request)
	if request.Name != "initial" {
		t.Fatalf("initial-list request = %q, want initial", request.Name)
	}
	if priority != handler.LowPriority {
		t.Fatalf("initial-list priority = %d, want %d", priority, handler.LowPriority)
	}
}

func TestNamespaceEventHandlerPromotesAlreadyQueuedNamespace(t *testing.T) {
	t.Parallel()

	queue := priorityqueue.New[reconcile.Request](t.Name())
	t.Cleanup(queue.ShutDown)
	namespace := testNamespace("promoted")
	(&handler.EnqueueRequestForObject{}).Generic(
		context.Background(),
		event.GenericEvent{Object: namespace},
		queue,
	)

	(&namespaceEventHandler{}).Create(
		context.Background(),
		event.CreateEvent{Object: namespace},
		queue,
	)

	request, priority, shutdown := queue.GetWithPriority()
	if shutdown {
		t.Fatal("queue shut down before returning the promoted Namespace")
	}
	queue.Done(request)
	if request.Name != "promoted" {
		t.Fatalf("promoted request = %q, want promoted", request.Name)
	}
	if priority != liveNamespaceCreatePriority {
		t.Fatalf("promoted Namespace priority = %d, want %d", priority, liveNamespaceCreatePriority)
	}
	if queue.Len() != 0 {
		t.Fatalf("queue length after de-duplicated request = %d, want 0", queue.Len())
	}
}

func TestNamespaceEventHandlerFallsBackWithoutPriorityQueue(t *testing.T) {
	t.Parallel()

	queue := workqueue.NewTypedRateLimitingQueue(
		workqueue.DefaultTypedControllerRateLimiter[reconcile.Request](),
	)
	t.Cleanup(queue.ShutDown)
	(&namespaceEventHandler{}).Create(context.Background(), event.CreateEvent{
		Object: testNamespace("fallback"),
	}, queue)

	request, shutdown := queue.Get()
	if shutdown {
		t.Fatal("queue shut down before returning the fallback request")
	}
	queue.Done(request)
	if request.Name != "fallback" {
		t.Fatalf("fallback request = %q, want fallback", request.Name)
	}
}

func testNamespace(name string) *corev1.Namespace {
	return &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}
}
