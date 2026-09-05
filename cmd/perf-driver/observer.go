package main

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	toolscache "k8s.io/client-go/tools/cache"

	"github.com/RyanWang945/kubernetes-registry-secret-controller/internal/controller"
)

type convergenceObserver struct {
	secretInformer         toolscache.SharedIndexInformer
	serviceAccountInformer toolscache.SharedIndexInformer
}

func startConvergenceObserver(
	ctx context.Context,
	clientset kubernetes.Interface,
	runLabelSelector string,
	tracker *convergenceTracker,
) (*convergenceObserver, error) {
	secretSelector := fields.OneTermEqualSelector("metadata.name", controller.DefaultManagedSecretName).String()
	secretInformer := toolscache.NewSharedIndexInformer(
		&toolscache.ListWatch{
			ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
				options.FieldSelector = secretSelector
				return clientset.CoreV1().Secrets(metav1.NamespaceAll).List(ctx, options)
			},
			WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
				options.FieldSelector = secretSelector
				return clientset.CoreV1().Secrets(metav1.NamespaceAll).Watch(ctx, options)
			},
		},
		&corev1.Secret{},
		0,
		toolscache.Indexers{},
	)
	serviceAccountInformer := toolscache.NewSharedIndexInformer(
		&toolscache.ListWatch{
			ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
				options.LabelSelector = runLabelSelector
				return clientset.CoreV1().ServiceAccounts(metav1.NamespaceAll).List(ctx, options)
			},
			WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
				options.LabelSelector = runLabelSelector
				return clientset.CoreV1().ServiceAccounts(metav1.NamespaceAll).Watch(ctx, options)
			},
		},
		&corev1.ServiceAccount{},
		0,
		toolscache.Indexers{},
	)

	if _, err := secretInformer.AddEventHandler(toolscache.ResourceEventHandlerFuncs{
		AddFunc: func(object any) {
			if secret, ok := object.(*corev1.Secret); ok {
				tracker.observeSecret(secret, time.Now())
			}
		},
		UpdateFunc: func(_, object any) {
			if secret, ok := object.(*corev1.Secret); ok {
				tracker.observeSecret(secret, time.Now())
			}
		},
	}); err != nil {
		return nil, fmt.Errorf("register Secret observer: %w", err)
	}
	if _, err := serviceAccountInformer.AddEventHandler(toolscache.ResourceEventHandlerFuncs{
		AddFunc: func(object any) {
			if serviceAccount, ok := object.(*corev1.ServiceAccount); ok {
				tracker.observeServiceAccount(serviceAccount, time.Now())
			}
		},
		UpdateFunc: func(_, object any) {
			if serviceAccount, ok := object.(*corev1.ServiceAccount); ok {
				tracker.observeServiceAccount(serviceAccount, time.Now())
			}
		},
	}); err != nil {
		return nil, fmt.Errorf("register ServiceAccount observer: %w", err)
	}

	observer := &convergenceObserver{
		secretInformer:         secretInformer,
		serviceAccountInformer: serviceAccountInformer,
	}
	go secretInformer.Run(ctx.Done())
	go serviceAccountInformer.Run(ctx.Done())
	if !toolscache.WaitForCacheSync(ctx.Done(), secretInformer.HasSynced, serviceAccountInformer.HasSynced) {
		return nil, errorsFromContext(ctx, "synchronize performance observers")
	}
	return observer, nil
}

func errorsFromContext(ctx context.Context, operation string) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("%s: %w", operation, err)
	}
	return fmt.Errorf("%s: cache stopped", operation)
}
