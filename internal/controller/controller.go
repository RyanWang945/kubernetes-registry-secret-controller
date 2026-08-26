package controller

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/informers"
	coreinformers "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"

	"github.com/RyanWang945/kubernetes-registry-secret-controller/internal/config"
)

const (
	DefaultControllerNamespace = "registry-secret-controller-system"
	DefaultConfigMapName       = "registry-secret-controller-config"
	DefaultResourceWorkers     = 2

	resourceQueueName = "namespace-resource-sync"
)

// NamespaceSyncer owns the resource-level reconciliation for one namespace.
// The listener and queue layer deliberately pass only a key, so implementations
// always read the latest configuration and credential snapshots.
type NamespaceSyncer interface {
	SyncNamespace(ctx context.Context, namespace string) error
}

type NamespaceSyncFunc func(ctx context.Context, namespace string) error

func (f NamespaceSyncFunc) SyncNamespace(ctx context.Context, namespace string) error {
	return f(ctx, namespace)
}

type Options struct {
	ControllerNamespace string
	ConfigMapName       string
	ResourceWorkers     int
	Logger              *slog.Logger
}

func (o Options) withDefaults() Options {
	if o.ControllerNamespace == "" {
		o.ControllerNamespace = DefaultControllerNamespace
	}
	if o.ConfigMapName == "" {
		o.ConfigMapName = DefaultConfigMapName
	}
	if o.ResourceWorkers == 0 {
		o.ResourceWorkers = DefaultResourceWorkers
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	return o
}

// Controller wires Kubernetes List/Watch events to a namespace work queue.
// It is safe to construct before the fixed ConfigMap exists; a later valid Add
// event starts reconciliation without restarting the process.
type Controller struct {
	options Options
	logger  *slog.Logger

	configParser *config.Parser
	configStore  *config.Store
	syncer       NamespaceSyncer

	coreFactory   informers.SharedInformerFactory
	configFactory informers.SharedInformerFactory

	namespaceInformer      coreinformers.NamespaceInformer
	serviceAccountInformer coreinformers.ServiceAccountInformer
	configMapInformer      coreinformers.ConfigMapInformer

	resourceQueue workqueue.TypedRateLimitingInterface[string]

	cacheReady  atomic.Bool
	synced      chan struct{}
	syncedOnce  sync.Once
	configReady chan struct{}
	configOnce  sync.Once
	runStarted  atomic.Bool
}

func New(
	client kubernetes.Interface,
	configStore *config.Store,
	syncer NamespaceSyncer,
	options Options,
) (*Controller, error) {
	if client == nil {
		return nil, errors.New("kubernetes client must not be nil")
	}
	if configStore == nil {
		return nil, errors.New("config store must not be nil")
	}
	if syncer == nil {
		return nil, errors.New("namespace syncer must not be nil")
	}

	options = options.withDefaults()
	if options.ResourceWorkers < 1 {
		return nil, fmt.Errorf("resource workers must be at least one, got %d", options.ResourceWorkers)
	}

	parser, err := config.NewParser(options.ControllerNamespace)
	if err != nil {
		return nil, err
	}

	coreFactory := informers.NewSharedInformerFactory(client, 0)
	configFactory := informers.NewSharedInformerFactoryWithOptions(
		client,
		0,
		informers.WithNamespace(options.ControllerNamespace),
		informers.WithTweakListOptions(func(listOptions *metav1.ListOptions) {
			listOptions.FieldSelector = fields.OneTermEqualSelector(
				"metadata.name",
				options.ConfigMapName,
			).String()
		}),
	)

	controller := &Controller{
		options:                options,
		logger:                 options.Logger.With("component", "resource-controller"),
		configParser:           parser,
		configStore:            configStore,
		syncer:                 syncer,
		coreFactory:            coreFactory,
		configFactory:          configFactory,
		namespaceInformer:      coreFactory.Core().V1().Namespaces(),
		serviceAccountInformer: coreFactory.Core().V1().ServiceAccounts(),
		configMapInformer:      configFactory.Core().V1().ConfigMaps(),
		resourceQueue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{Name: resourceQueueName},
		),
		synced:      make(chan struct{}),
		configReady: make(chan struct{}),
	}

	if err := controller.registerEventHandlers(); err != nil {
		controller.resourceQueue.ShutDown()
		return nil, err
	}

	return controller, nil
}

func (c *Controller) registerEventHandlers() error {
	if _, err := c.configMapInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: c.onConfigMapAdd,
		UpdateFunc: func(_, newObject any) {
			c.onConfigMapAdd(newObject)
		},
		DeleteFunc: c.onConfigMapDelete,
	}); err != nil {
		return fmt.Errorf("register ConfigMap event handler: %w", err)
	}

	if _, err := c.namespaceInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: c.onNamespaceAdd,
	}); err != nil {
		return fmt.Errorf("register Namespace event handler: %w", err)
	}

	if _, err := c.serviceAccountInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: c.onServiceAccountChange,
		UpdateFunc: func(_, newObject any) {
			c.onServiceAccountChange(newObject)
		},
	}); err != nil {
		return fmt.Errorf("register ServiceAccount event handler: %w", err)
	}

	return nil
}

// CachesSynced is closed once all initial Lists have completed. Workers start
// only after this point and after the first valid ConfigMap has been loaded.
func (c *Controller) CachesSynced() <-chan struct{} {
	return c.synced
}

// EnqueueNamespace is used both by resource watches and, later, successful
// credential refreshes. WorkQueue de-duplicates concurrent additions.
func (c *Controller) EnqueueNamespace(namespace string) {
	if namespace == "" {
		return
	}
	c.resourceQueue.Add(namespace)
}

func (c *Controller) Run(ctx context.Context) error {
	if !c.runStarted.CompareAndSwap(false, true) {
		return errors.New("controller can only be run once")
	}

	c.coreFactory.Start(ctx.Done())
	c.configFactory.Start(ctx.Done())

	if !cache.WaitForCacheSync(
		ctx.Done(),
		c.namespaceInformer.Informer().HasSynced,
		c.serviceAccountInformer.Informer().HasSynced,
		c.configMapInformer.Informer().HasSynced,
	) {
		c.resourceQueue.ShutDown()
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("wait for informer caches: %w", err)
		}
		return errors.New("wait for informer caches: cache synchronization failed")
	}

	c.cacheReady.Store(true)
	c.syncedOnce.Do(func() { close(c.synced) })
	c.logger.Info("informer caches synchronized")

	select {
	case <-c.configReady:
	case <-ctx.Done():
		c.resourceQueue.ShutDown()
		return nil
	}

	c.enqueueCurrentTargets()
	c.logger.Info("starting resource workers", "workers", c.options.ResourceWorkers)

	var workers sync.WaitGroup
	workers.Add(c.options.ResourceWorkers)
	for workerID := range c.options.ResourceWorkers {
		go func() {
			defer workers.Done()
			c.runResourceWorker(ctx, workerID)
		}()
	}

	<-ctx.Done()
	c.resourceQueue.ShutDown()
	workers.Wait()
	c.logger.Info("resource controller stopped")
	return nil
}

func (c *Controller) runResourceWorker(ctx context.Context, workerID int) {
	c.logger.Debug("resource worker started", "worker", workerID)
	defer c.logger.Debug("resource worker stopped", "worker", workerID)

	for c.processNextResource(ctx) {
	}
}

func (c *Controller) processNextResource(ctx context.Context) bool {
	namespace, shutdown := c.resourceQueue.Get()
	if shutdown {
		return false
	}
	defer c.resourceQueue.Done(namespace)

	if err := c.syncer.SyncNamespace(ctx, namespace); err != nil {
		if ctx.Err() == nil {
			c.logger.Error(
				"namespace reconciliation failed",
				"namespace", namespace,
				"requeues", c.resourceQueue.NumRequeues(namespace),
				"error", err,
			)
			c.resourceQueue.AddRateLimited(namespace)
		}
		return true
	}

	c.resourceQueue.Forget(namespace)
	return true
}

func (c *Controller) onConfigMapAdd(object any) {
	configMap, ok := object.(*corev1.ConfigMap)
	if !ok || configMap.Namespace != c.options.ControllerNamespace || configMap.Name != c.options.ConfigMapName {
		return
	}

	candidate, err := c.configParser.Parse(configMap.Data)
	if err != nil {
		c.logger.Error(
			"rejected invalid ConfigMap; retaining the last valid configuration",
			"namespace", configMap.Namespace,
			"name", configMap.Name,
			"error", err,
		)
		return
	}

	result := c.configStore.Apply(candidate)
	c.configOnce.Do(func() { close(c.configReady) })
	if !result.Changed {
		c.logger.Debug(
			"ConfigMap has no semantic changes",
			"namespace", configMap.Namespace,
			"name", configMap.Name,
			"generation", result.Current.Generation,
		)
		return
	}

	c.logger.Info(
		"applied valid ConfigMap",
		"namespace", configMap.Namespace,
		"name", configMap.Name,
		"generation", result.Current.Generation,
		"registries", len(result.Current.Registries),
	)
	if c.cacheReady.Load() {
		c.enqueueAffectedNamespaces(result)
	}
}

func (c *Controller) onConfigMapDelete(object any) {
	configMap, ok := deletedConfigMap(object)
	if !ok || configMap.Namespace != c.options.ControllerNamespace || configMap.Name != c.options.ConfigMapName {
		return
	}

	c.logger.Error(
		"fixed ConfigMap was deleted; retaining the last valid configuration",
		"namespace", configMap.Namespace,
		"name", configMap.Name,
	)
}

func (c *Controller) onNamespaceAdd(object any) {
	namespace, ok := object.(*corev1.Namespace)
	if !ok {
		return
	}
	snapshot, loaded := c.configStore.Load()
	if loaded && snapshot.Namespaces.Matches(namespace.Name) {
		c.EnqueueNamespace(namespace.Name)
	}
}

func (c *Controller) onServiceAccountChange(object any) {
	serviceAccount, ok := object.(*corev1.ServiceAccount)
	if !ok {
		return
	}
	snapshot, loaded := c.configStore.Load()
	if !loaded || !snapshot.Namespaces.Matches(serviceAccount.Namespace) {
		return
	}
	if snapshot.ServiceAccounts.Matches(serviceAccount.Name) {
		c.EnqueueNamespace(serviceAccount.Namespace)
	}
}

func (c *Controller) enqueueCurrentTargets() {
	snapshot, loaded := c.configStore.Load()
	if !loaded {
		c.logger.Info("informer caches synchronized; waiting for a valid ConfigMap")
		return
	}
	c.enqueueNamespacesMatching(func(namespace string) bool {
		return snapshot.Namespaces.Matches(namespace)
	})
}

func (c *Controller) enqueueAffectedNamespaces(result config.ApplyResult) {
	c.enqueueNamespacesMatching(func(namespace string) bool {
		return result.Current.Namespaces.Matches(namespace) ||
			(result.HadPrevious && result.Previous.Namespaces.Matches(namespace))
	})
}

func (c *Controller) enqueueNamespacesMatching(matches func(namespace string) bool) {
	namespaces, err := c.namespaceInformer.Lister().List(labels.Everything())
	if err != nil {
		c.logger.Error("list namespaces from informer cache", "error", err)
		return
	}
	for _, namespace := range namespaces {
		if matches(namespace.Name) {
			c.EnqueueNamespace(namespace.Name)
		}
	}
}

func deletedConfigMap(object any) (*corev1.ConfigMap, bool) {
	if configMap, ok := object.(*corev1.ConfigMap); ok {
		return configMap, true
	}
	tombstone, ok := object.(cache.DeletedFinalStateUnknown)
	if !ok {
		return nil, false
	}
	configMap, ok := tombstone.Obj.(*corev1.ConfigMap)
	return configMap, ok
}
