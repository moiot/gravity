package controller

import (
	"fmt"
	"reflect"
	"sort"
	"sync/atomic"
	"time"

	"github.com/juju/errors"
	"github.com/moiot/gravity/metrics"
	log "github.com/sirupsen/logrus"
	"k8s.io/client-go/informers"

	clusterapi "github.com/moiot/gravity/k8s_operator/pkg/apis/cluster/v1alpha1"
	pipeapi "github.com/moiot/gravity/k8s_operator/pkg/apis/pipeline/v1alpha1"
	clusterclient "github.com/moiot/gravity/k8s_operator/pkg/client/cluster/clientset/versioned"
	"github.com/moiot/gravity/k8s_operator/pkg/client/cluster/clientset/versioned/scheme"
	clusterinformer "github.com/moiot/gravity/k8s_operator/pkg/client/cluster/informers/externalversions/cluster/v1alpha1"
	clusterlister "github.com/moiot/gravity/k8s_operator/pkg/client/cluster/listers/cluster/v1alpha1"
	pipeclient "github.com/moiot/gravity/k8s_operator/pkg/client/pipeline/clientset/versioned"
	pipeinformer "github.com/moiot/gravity/k8s_operator/pkg/client/pipeline/informers/externalversions/pipeline/v1alpha1"
	pipelister "github.com/moiot/gravity/k8s_operator/pkg/client/pipeline/listers/pipeline/v1alpha1"

	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
)

const controllerAgentName = "drc-controller"

type Interface interface {
	GetCluster() *clusterapi.DrcCluster
	GetK8Pipeline(namespace, pipelineName string) (*pipeapi.DrcPipeline, error)
	ListK8Pipelines(namespace string) ([]*pipeapi.DrcPipeline, error)

	Reset(pipeline *pipeapi.DrcPipeline) error
}

type ClusterController struct {
	namespace string
	pm        *PipelineManager

	kubeclientset kubernetes.Interface
	clusterClient clusterclient.Interface
	pipeClient    pipeclient.Interface

	cluster *atomic.Value

	clusterLister clusterlister.DrcClusterLister
	clusterSynced cache.InformerSynced

	pipeLister pipelister.DrcPipelineLister
	pipeSynced cache.InformerSynced
	workqueue  workqueue.RateLimitingInterface

	recorder record.EventRecorder
}

func NewClusterController(namespace string, kubeInformerFactory informers.SharedInformerFactory,
	kubeclientset kubernetes.Interface,
	clusterClient clusterclient.Interface, clusterInformer clusterinformer.DrcClusterInformer,
	pipeClient pipeclient.Interface, pipeInformer pipeinformer.DrcPipelineInformer) *ClusterController {

	// Create event broadcaster
	// Add drc types to the default Kubernetes Scheme so Events can be
	// logged for drc types.
	scheme.AddToScheme(scheme.Scheme)

	log.Info("Creating event broadcaster")
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(log.Infof)
	eventBroadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: kubeclientset.CoreV1().Events(namespace)})
	recorder := eventBroadcaster.NewRecorder(scheme.Scheme, corev1.EventSource{Component: controllerAgentName})

	cc := &ClusterController{
		namespace:     namespace,
		kubeclientset: kubeclientset,
		clusterClient: clusterClient,
		pipeClient:    pipeClient,
		clusterLister: clusterInformer.Lister(),
		clusterSynced: clusterInformer.Informer().HasSynced,
		pipeLister:    pipeInformer.Lister(),
		pipeSynced:    pipeInformer.Informer().HasSynced,
		workqueue:     workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "Cluster"),
		recorder:      recorder,
		cluster:       &atomic.Value{},
	}

	cc.pm = newPipelineController(namespace,
		kubeclientset, pipeClient,
		kubeInformerFactory.Apps().V1().Deployments(),
		kubeInformerFactory.Core().V1().Pods(),
		cc.pipeLister,
		kubeInformerFactory.Core().V1().ConfigMaps().Lister(),
	)

	clusterInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		UpdateFunc: func(old, new interface{}) {
			newConfig := new.(*clusterapi.DrcCluster)
			//oldConfig := old.(*clusterapi.DrcCluster)
			log.Infof("enqueue cluster due to update")
			cc.enqueue(newConfig)
		},
	})

	pipeInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			p := obj.(*pipeapi.DrcPipeline)
			log.Infof("Pipeline[%s] created, enqueue", p.Name)
			cc.pm.enqueuePipeline(p)
			cc.enqueueByPipe(p)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			oldPipe := oldObj.(*pipeapi.DrcPipeline)
			newPipe := newObj.(*pipeapi.DrcPipeline)
			if cc.pm.needSync(oldPipe, newPipe) {
				log.Infof("Pipeline[%s] updated, enqueue", oldPipe.Name)
				cc.pm.enqueuePipeline(newPipe)
				cc.enqueueByPipe(newPipe)
			}
		},
		DeleteFunc: func(obj interface{}) {
			p := obj.(*pipeapi.DrcPipeline)
			log.Infof("Pipeline[%s] deleted, enqueue", p.Name)
			cc.pm.enqueuePipeline(p)
			cc.enqueueByPipe(p)
		},
	})

	return cc
}

func (cc *ClusterController) enqueueByPipe(pipeline *pipeapi.DrcPipeline) {
	if cc.cluster.Load() == nil {
		log.Warnf("[ClusterController.enqueueByPipe] cluster not initialized, ignore %s", pipeline.Name)
		return
	}
	ownerRef := metav1.GetControllerOf(pipeline)
	if ownerRef == nil {
		pipeline.OwnerReferences = []metav1.OwnerReference{
			*metav1.NewControllerRef(cc.GetCluster(), clusterapi.SchemeGroupVersion.WithKind(clusterapi.ClusterResourceKind)),
		}
		_, err := cc.pipeClient.DrcV1alpha1().DrcPipelines(pipeline.Namespace).Update(pipeline)
		if err != nil {
			log.Error(errors.Trace(err))
		}
		return
	}

	if ownerRef.Kind != clusterapi.ClusterResourceKind {
		log.Errorf("[ClusterController.enqueueByPipe] pipeline %s not owned by cluster", pipeline.Name)
		return
	}

	cc.enqueue(cc.GetCluster())
}

func (cc *ClusterController) Reset(pipeline *pipeapi.DrcPipeline) error {
	return cc.pm.Reset(pipeline)
}

func (cc *ClusterController) GetK8Pipeline(namespace, pipelineName string) (*pipeapi.DrcPipeline, error) {
	return cc.pipeLister.DrcPipelines(namespace).Get(pipelineName)
}

func (cc *ClusterController) ListK8Pipelines(namespace string) ([]*pipeapi.DrcPipeline, error) {
	return cc.pipeLister.DrcPipelines(namespace).List(labels.Everything())
}

func (cc *ClusterController) GetCluster() *clusterapi.DrcCluster {
	return cc.cluster.Load().(*clusterapi.DrcCluster)
}

func (cc *ClusterController) enqueue(cluster *clusterapi.DrcCluster) {
	key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(cluster)
	if err != nil {
		log.Error(errors.Trace(err))
		return
	}
	cc.workqueue.AddRateLimited(key)
}

func (cc *ClusterController) Run(threadness int, stopCh <-chan struct{}) error {
	log.Info("Starting DRC Cluster controller")

	log.Info("[ClusterController.Run] waiting for informer caches to sync")

	if ok := cache.WaitForCacheSync(stopCh, cc.clusterSynced, cc.pipeSynced); !ok {
		return fmt.Errorf("failed to wait for caches to sync")
	}

	ret, err := cc.clusterLister.DrcClusters(cc.namespace).List(labels.Everything())
	if err != nil {
		log.Fatalf("[ClusterController.Run] error list cluster: %s", err)
	}

	if len(ret) != 1 {
		log.Fatalf("[ClusterController.Run] expect exactly one cluster object per namespace, actually %d", len(ret))
	}

	cc.cluster.Store(ret[0])

	log.Infof("[ClusterController.Run] Starting workers")

	go func() {
		<-stopCh
		log.Info("[ClusterController] shutdown work queue")
		cc.workqueue.ShutDown()
	}()

	for i := 0; i < threadness; i++ {
		go wait.Until(cc.runWorker, time.Second, stopCh)
	}
	log.Infof("[ClusterController.Run] Started workers")

	if err := cc.pm.Run(threadness, stopCh); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (cc *ClusterController) runWorker() {
	for cc.processNextWorkItem() {
	}
}

func (cc *ClusterController) processNextWorkItem() bool {
	key, shutdown := cc.workqueue.Get()

	if shutdown {
		return false
	}

	// We wrap this block in a func so we can defer c.workqueue.Done.
	err := func(key string) error {
		defer cc.workqueue.Done(key)

		if err := cc.syncHandler(key); err != nil {
			return fmt.Errorf("error syncing '%s': %s", key, err.Error())
		}

		// Finally, if no error occurs we Forget this item so it does not
		// get queued again until another change happens.
		cc.workqueue.Forget(key)
		log.Infof("[ClusterController.processNextWorkItem] Successfully synced '%s'", key)
		return nil
	}(key.(string))

	if err != nil {
		log.Error(err)
		return false
	}

	return true
}

func (cc *ClusterController) syncHandler(key string) error {
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return errors.Trace(err)
	}

	start := time.Now()
	defer func() {
		result := "success"
		if err != nil {
			result = "error"
		}
		metrics.OperatorSyncCount.WithLabelValues(name, "cluster", result).Add(1)

		c := cc.GetCluster()
		metrics.OperatorClusterStatus.WithLabelValues("total").Set(float64(c.Status.Pipelines))
		metrics.OperatorClusterStatus.WithLabelValues("available").Set(float64(c.Status.AvailablePipelines))
		metrics.OperatorClusterStatus.WithLabelValues("updated").Set(float64(c.Status.UpdatedPipelines))
		metrics.OperatorClusterStatus.WithLabelValues("unavailable").Set(float64(c.Status.UnavailablePipelines))

		metrics.OperatorScheduleHistogram.WithLabelValues(name, "cluster").Observe(time.Since(start).Seconds())
	}()

	cluster, err := cc.clusterClient.DrcV1alpha1().DrcClusters(namespace).Get(name, metav1.GetOptions{})
	if k8serrors.IsNotFound(err) {
		log.Infof("cluster %s has been deleted", key)
		return nil
	}
	if err != nil {
		return errors.Trace(err)
	}

	c := cluster.DeepCopy()

	pipelines, err := cc.pipeLister.DrcPipelines(namespace).List(labels.Everything())
	if err != nil {
		return errors.Annotatef(err, "fail list all pipeline")
	}
	sort.Slice(pipelines, func(i, j int) bool {
		return pipelines[i].Name < pipelines[j].Name
	})

	status, err := cc.syncStatus(c, pipelines)
	if err != nil {
		return errors.Trace(err)
	}

	if !reflect.DeepEqual(c.Status, status) {
		c.Status = status
		c, err = cc.clusterClient.DrcV1alpha1().DrcClusters(namespace).UpdateStatus(c)
		if err != nil {
			return errors.Trace(err)
		}
		cc.cluster.Store(c)
	}

	if c.Status.UpdatedPipelines < c.Status.Pipelines {
		err = cc.upgrade(c, pipelines)
		if err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

func (cc *ClusterController) upgrade(c *clusterapi.DrcCluster, pipelines []*pipeapi.DrcPipeline) error {
	maxRolling, err := intstr.GetValueFromIntOrPercent(c.Spec.Rolling, int(c.Status.Pipelines), true)
	if err != nil {
		return errors.Trace(err)
	}
	chosen := 0
	for _, p := range pipelines {
		rule := c.FindDeploymentRule(p.Name)
		if rule == nil {
			return errors.Errorf("failed to find deployment rule, pipeline: %v", p.Name)
		}

		if rule.Image == p.Spec.Image && reflect.DeepEqual(rule.Command, p.Spec.Command) &&
			(!p.Status.Available() || p.Status.Image != p.Spec.Image || !reflect.DeepEqual(p.Status.Command, p.Spec.Command)) {
			log.Infof("[ClusterController.upgrade] pipeline %s is upgrading", p.Name)
			chosen += 1
		} else if rule.Image != p.Spec.Image || !reflect.DeepEqual(rule.Command, p.Spec.Command) {
			chosen += 1
			log.Infof("[ClusterController] upgrade %s from %s(%s) to %s(%s)", p.Name, p.Spec.Image, p.Spec.Command, rule.Image, rule.Command)
			p.Spec.Image = rule.Image
			p.Spec.Command = rule.Command
			_, err = cc.pipeClient.DrcV1alpha1().DrcPipelines(p.Namespace).Update(p)
			if err != nil {
				return errors.Annotatef(err, "[ClusterController] error update pipeline %s", p.Name)
			}
			cc.recorder.Eventf(c, corev1.EventTypeNormal, "Upgraded", "Upgraded pipeline %s to %s(%s)", p.Name, rule.Image, rule.Command)
		}

		if chosen == maxRolling {
			break
		}
	}
	return nil
}

func (cc *ClusterController) syncStatus(c *clusterapi.DrcCluster, pipelines []*pipeapi.DrcPipeline) (clusterapi.DrcClusterStatus, error) {
	status := clusterapi.DrcClusterStatus{
		ObservedGeneration: c.Generation,
		Pipelines:          int32(len(pipelines)),
	}
	for i := range c.Status.Conditions {
		status.Conditions = append(status.Conditions, c.Status.Conditions[i])
	}

	var updated, available int32
	for _, p := range pipelines {
		rule := c.FindDeploymentRule(p.Name)
		if rule == nil {
			return status, errors.Errorf("failed to find deployment rule, pipeline: %v", p.Name)
		}

		if rule.Image == p.Spec.Image && reflect.DeepEqual(rule.Command, p.Spec.Command) {
			updated += 1
		}

		if p.Status.Available() {
			available += 1
		}
	}
	status.UpdatedPipelines = updated
	status.AvailablePipelines = available
	status.UnavailablePipelines = status.Pipelines - status.AvailablePipelines

	cond := clusterapi.DrcClusterCondition{
		Type:               clusterapi.ClusterAvailable,
		LastUpdateTime:     metav1.Now(),
		LastTransitionTime: metav1.Now(),
	}
	if status.UnavailablePipelines == 0 {
		cond.Status = corev1.ConditionTrue
		cond.Reason = "EveryPipelineAvailable"
		cond.Message = "Cluster has every pipeline available"
	} else {
		cond.Status = corev1.ConditionFalse
		cond.Reason = "PipelineUnavailable"
		cond.Message = "Cluster has unavailable pipeline"
	}
	setClusterCondition(&status, cond)

	cond = clusterapi.DrcClusterCondition{
		Type:               clusterapi.ClusterUpToDate,
		LastUpdateTime:     metav1.Now(),
		LastTransitionTime: metav1.Now(),
	}
	if status.UpdatedPipelines == status.Pipelines {
		cond.Status = corev1.ConditionTrue
		cond.Reason = "ClusterUpToDate"
		cond.Message = "Cluster has every pipeline updated"
	} else {
		cond.Status = corev1.ConditionFalse
		cond.Reason = "ClusterUpgrading"
		cond.Message = "Cluster is upgrading pipeline"
	}
	setClusterCondition(&status, cond)

	return status, nil
}

func setClusterCondition(status *clusterapi.DrcClusterStatus, condition clusterapi.DrcClusterCondition) {
	curCond := status.Condition(condition.Type)
	if curCond != nil && curCond.Status == condition.Status && curCond.Reason == condition.Reason {
		return
	}

	if curCond != nil && curCond.Status == condition.Status {
		condition.LastTransitionTime = curCond.LastTransitionTime
	}

	for i := range status.Conditions {
		if status.Conditions[i].Type == condition.Type {
			status.Conditions[i] = condition
			return
		}
	}

	status.Conditions = append(status.Conditions, condition)
}
