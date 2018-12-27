package controller

import (
	"fmt"
	"reflect"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/moiot/gravity/metrics"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"

	"encoding/json"
	"io/ioutil"
	"net/http"

	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"

	appsinformers "k8s.io/client-go/informers/apps/v1"
	coreinformers "k8s.io/client-go/informers/core/v1"

	api "github.com/moiot/gravity/k8s_operator/pkg/apis/pipeline/v1alpha1"
	client "github.com/moiot/gravity/k8s_operator/pkg/client/pipeline/clientset/versioned"
	"github.com/moiot/gravity/k8s_operator/pkg/client/pipeline/clientset/versioned/scheme"
	"github.com/moiot/gravity/pkg/core"

	"github.com/juju/errors"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	appslisters "k8s.io/client-go/listers/apps/v1"
	corelisters "k8s.io/client-go/listers/core/v1"

	"k8s.io/apimachinery/pkg/labels"

	listers "github.com/moiot/gravity/k8s_operator/pkg/client/pipeline/listers/pipeline/v1alpha1"
)

const (
	// ErrResourceExists is used as part of the Event 'reason' when a Pipeline fails
	// to sync due to a ConfigMap of the same name already existing.
	ErrResourceExists = "ErrResourceExists"

	// ErrResourceExists is used as part of the Event 'reason' when a Pipeline fails
	// to sync due to a ConfigMap of the same name already existing.
	ErrSyncFailed = "ErrSyncFailed"

	// MessageResourceExists is the message used for Events when a resource
	// fails to sync due to a Deployment already existing
	MessageResourceExists = "ConfigMap %s already exists and is not managed by Pipeline"

	// MessageResourceSynced is the message used for an Event fired when a Pipeline
	// is synced successfully
	MessageSyncFailed = "Pipeline sync failed, err: %s"
)

type PipelineManager struct {
	namespace string

	kubeclientset kubernetes.Interface
	pipeclientset client.Interface

	deploymentLister appslisters.DeploymentLister
	deploymentSynced cache.InformerSynced

	podLister corelisters.PodLister
	podSynced cache.InformerSynced

	pipelinesLister listers.DrcPipelineLister
	configMapLister corelisters.ConfigMapLister

	workqueue workqueue.RateLimitingInterface
	// recorder is an event recorder for recording Event resources to the
	// Kubernetes API.
	recorder record.EventRecorder
}

func newPipelineController(
	namespace string,
	kubeclientset kubernetes.Interface,
	pipeclientset client.Interface,
	deploymentInformer appsinformers.DeploymentInformer,
	podInformer coreinformers.PodInformer,
	pipeLister listers.DrcPipelineLister,
	configMapLister corelisters.ConfigMapLister) *PipelineManager {

	// Create event broadcaster
	// Add drc types to the default Kubernetes Scheme so Events can be
	// logged for drc types.
	scheme.AddToScheme(scheme.Scheme)

	log.Info("Creating event broadcaster")
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(log.Infof)
	eventBroadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: kubeclientset.CoreV1().Events(namespace)})
	recorder := eventBroadcaster.NewRecorder(scheme.Scheme, corev1.EventSource{Component: controllerAgentName})

	controller := &PipelineManager{
		kubeclientset: kubeclientset,
		pipeclientset: pipeclientset,

		deploymentLister: deploymentInformer.Lister(),
		deploymentSynced: deploymentInformer.Informer().HasSynced,

		podLister: podInformer.Lister(),
		podSynced: podInformer.Informer().HasSynced,

		pipelinesLister: pipeLister,

		workqueue: workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "Pipeline"),

		configMapLister: configMapLister,
		recorder:        recorder,
	}

	// Set up an event handler for when Deployment resources change. This
	// handler will lookup the owner of the given Deployment, and if it is
	// owned by a Pipeline resource will enqueue that Pipeline resource for
	// processing. This way, we don't need to implement custom logic for
	// handling Deployment resources. More info on this pattern:
	// https://github.com/kubernetes/community/blob/8cafef897a22026d42f5e5bb3f104febe7e29830/contributors/devel/controllers.md
	deploymentInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: controller.handleObject,
		UpdateFunc: func(old, new interface{}) {
			// update position when re-sync
			controller.handleObject(new)
		},
		DeleteFunc: controller.handleObject,
	})

	return controller
}

// Run will set up the event handlers for types we are interested in, as well
// as syncing informer caches and starting workers. It will block until stopCh
// is closed, at which point it will shutdown the workqueue and wait for
// workers to finish processing their current work items.
func (pm *PipelineManager) Run(threadiness int, stopCh <-chan struct{}) error {
	// Start the informer factories to begin populating the informer caches
	log.Info("[PipelineManager.Run] Starting...")

	// Wait for the caches to be synced before starting workers
	log.Info("[PipelineManager.Run] Waiting for informer caches to sync")

	if ok := cache.WaitForCacheSync(stopCh, pm.deploymentSynced, pm.podSynced); !ok {
		return fmt.Errorf("[PipelineManager.Run] failed to wait for caches to sync")
	}

	log.Info("[PipelineManager.Run] Starting workers")
	go func() {
		<-stopCh
		log.Info("[PipelineManager] shutdown work queue")
		pm.workqueue.ShutDown()
	}()

	// Launch two workers to process Pipeline resources
	for i := 0; i < threadiness; i++ {
		go wait.Until(pm.runWorker, time.Second, stopCh)
	}

	log.Info("[PipelineManager.Run] Started workers")
	return nil
}

// runWorker is a long-running function that will continually call the
// processNextWorkItem function in order to read and process a message on the
// workqueue.
func (pm *PipelineManager) runWorker() {
	for pm.processNextWorkItem() {
	}
}

func (pm *PipelineManager) processNextWorkItem() bool {
	obj, shutdown := pm.workqueue.Get()

	if shutdown {
		return false
	}

	// We wrap this block in a func so we can defer c.workqueue.Done.
	err := func(obj interface{}) error {
		// We call Done here so the workqueue knows we have finished
		// processing this item. We also must remember to call Forget if we
		// do not want this work item being re-queued. For example, we do
		// not call Forget if a transient error occurs, instead the item is
		// put back on the workqueue and attempted again after a back-off
		// period.
		defer pm.workqueue.Done(obj)
		var key string
		var ok bool
		// We expect strings to come off the workqueue. These are of the
		// form namespace/name. We do this as the delayed nature of the
		// workqueue means the items in the informer cache may actually be
		// more up to date that when the item was initially put onto the
		// workqueue.
		if key, ok = obj.(string); !ok {
			// As the item in the workqueue is actually invalid, we call
			// Forget here else we'd go into a loop of attempting to
			// process a work item that is invalid.
			pm.workqueue.Forget(obj)
			utilruntime.HandleError(fmt.Errorf("expected string in workqueue but got %#v", obj))
			return nil
		}
		// Run the syncHandler, passing it the namespace/name string of the
		// Pipeline resource to be synced.
		if err := pm.syncHandler(key); err != nil {
			return errors.Annotatef(err, "error syncing '%s': %s", key, errors.ErrorStack(err))
		}
		// Finally, if no error occurs we Forget this item so it does not
		// get queued again until another change happens.
		pm.workqueue.Forget(obj)
		log.Infof("[PipelineManager.processNextWorkItem] Successfully synced '%s'", key)
		return nil
	}(obj)

	if err != nil {
		utilruntime.HandleError(err)
		return true
	}

	return true
}

// syncHandler compares the actual state with the desired, and attempts to
// converge the two. It then updates the Status block of the Pipeline resource
// with the current status of the resource.
func (pm *PipelineManager) syncHandler(key string) error {
	// Convert the namespace/name string into a distinct namespace and name
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("invalid resource key: %s", key))
		return nil
	}

	start := time.Now()
	defer func() {
		result := "success"
		if err != nil {
			result = "error"
		}
		metrics.OperatorSyncCount.WithLabelValues(name, "pipeline", result).Add(1)
		metrics.OperatorScheduleHistogram.WithLabelValues(name, "pipeline").Observe(time.Since(start).Seconds())
	}()

	// Get the pipeline resource with this namespace/name
	pipeline, err := pm.pipelinesLister.DrcPipelines(namespace).Get(name)
	if err != nil {
		// The pipeline resource may no longer exist, in which case we stop
		// processing.
		if apierrors.IsNotFound(err) {
			utilruntime.HandleError(fmt.Errorf("pipeline '%s' in work queue no longer exists", key))
			return nil
		}
		return errors.Trace(err)
	}

	if pipeline.Spec.Image == "" || len(pipeline.Spec.Command) == 0 {
		log.Warnf("[PipelineManager.syncHandler] pipeline %s: no image or command in spec, ignore.", pipeline.Name)
	}

	deployment, err := pm.deploymentLister.Deployments(pipeline.Namespace).Get(pipeline.Name)
	// If the resource doesn't exist, we'll create it
	if apierrors.IsNotFound(err) {
		k8Deployment := pm.newDeployment(pipeline)
		deployment, err = pm.kubeclientset.AppsV1().Deployments(pipeline.Namespace).Create(k8Deployment)
	}

	// If an error occurs during Get/Create, we'll requeue the item so we can
	// attempt processing again later. This could have been caused by a
	// temporary network failure, or any other transient reason.
	if err != nil {
		metrics.OperatorPipelineUnavailable.WithLabelValues(name).Set(1)
		pm.recorder.Eventf(pipeline, corev1.EventTypeWarning, ErrSyncFailed, MessageSyncFailed, err.Error())
		return errors.Trace(err)
	}

	// If the Deployment is not controlled by this Pipeline resource, we should log
	// a warning to the event recorder. No need to retry until pipeline updated
	if !metav1.IsControlledBy(deployment, pipeline) {
		msg := fmt.Sprintf(MessageResourceExists, deployment.Name)
		pm.recorder.Event(pipeline, corev1.EventTypeWarning, ErrResourceExists, msg)
		return nil
	}

	metrics.OperatorPipelineUnavailable.WithLabelValues(name).Set(float64(deployment.Status.UnavailableReplicas))

	var expectedReplica int32 = 1
	if pipeline.Spec.Paused {
		expectedReplica = 0
	}
	if *deployment.Spec.Replicas != expectedReplica {
		deployment.Spec.Replicas = &expectedReplica
		deployment, err = pm.kubeclientset.AppsV1().Deployments(pipeline.Namespace).Update(deployment)
		if err != nil {
			pm.recorder.Eventf(pipeline, corev1.EventTypeWarning, ErrSyncFailed, MessageSyncFailed, err.Error())
			return errors.Trace(err)
		}
	}

	container := deployment.Spec.Template.Spec.Containers[0]
	if container.Image != pipeline.Spec.Image || !reflect.DeepEqual(container.Command, pipeline.Spec.Command) {
		deployment.Spec.Template.Spec.Containers[0].Image = pipeline.Spec.Image
		deployment.Spec.Template.Spec.Containers[0].Command = pipeline.Spec.Command
		deployment, err = pm.kubeclientset.AppsV1().Deployments(pipeline.Namespace).Update(deployment)
		if err != nil {
			pm.recorder.Eventf(pipeline, corev1.EventTypeWarning, ErrSyncFailed, MessageSyncFailed, err.Error())
			return errors.Trace(err)
		} else {
			pm.recorder.Eventf(pipeline, corev1.EventTypeNormal, "Upgraded", "Upgraded deployment %s to %s:%s", deployment.Name,
				pipeline.Spec.Image, pipeline.Spec.Command)
		}
	}

	pipeline, err = pm.updatePipelineStatus(pipeline, deployment)
	if err != nil {
		if pipeline != nil {
			pm.recorder.Eventf(pipeline, corev1.EventTypeWarning, ErrSyncFailed, MessageSyncFailed, err.Error())
		}
		return errors.Trace(err)
	}

	return nil
}

const containerPort = 8080

func pipeLabels(pipeline *api.DrcPipeline) map[string]string {
	return map[string]string{
		// https://kubernetes.io/docs/concepts/overview/working-with-objects/common-labels/#labels
		"app.kubernetes.io/name":     "drc",
		"app.kubernetes.io/instance": pipeline.Name,
	}
}

// newDeployment creates a new Deployment for a task. It also sets
// the appropriate OwnerReferences on the resource so handleObject can discover
// the Pipeline resource that 'owns' it.
func (pm *PipelineManager) newDeployment(pipeline *api.DrcPipeline) *appsv1.Deployment {
	lbls := pipeLabels(pipeline)

	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pipeline.Name,
			Namespace: pipeline.Namespace,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(pipeline, api.SchemeGroupVersion.WithKind(api.PipelineResourceKind)),
			},
			Labels: lbls,
		},
		Spec: appsv1.DeploymentSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: lbls,
			},
			Strategy: appsv1.DeploymentStrategy{
				Type: appsv1.RecreateDeploymentStrategyType,
			},
			MinReadySeconds: 10,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: lbls,
				},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyAlways,
					Volumes: []corev1.Volume{
						{
							Name: "config",
							VolumeSource: corev1.VolumeSource{
								ConfigMap: &corev1.ConfigMapVolumeSource{
									LocalObjectReference: corev1.LocalObjectReference{Name: pipeline.Name},
								},
							},
						},
					},
					Containers: []corev1.Container{
						{
							Name:    "drc",
							Image:   pipeline.Spec.Image,
							Command: pipeline.Spec.Command,
							Ports: []corev1.ContainerPort{
								{
									Name:          "http",
									ContainerPort: containerPort,
								},
							},
							LivenessProbe: &corev1.Probe{
								Handler: corev1.Handler{
									HTTPGet: &corev1.HTTPGetAction{
										Port: intstr.FromString("http"),
										Path: "/healthz",
									},
								},
								InitialDelaySeconds: 10,
								TimeoutSeconds:      5,
								PeriodSeconds:       10,
							},
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "config",
									MountPath: "/etc/drc",
								},
							},
							Resources: corev1.ResourceRequirements{ //TODO from tps config or metrics
								Requests: corev1.ResourceList{
									"cpu":    resource.MustParse("100m"),
									"memory": resource.MustParse("150M"),
								},
							},
						},
					},
				},
			},
		},
	}

	if pipeline.Spec.Paused {
		deployment.Spec.Replicas = int32Ptr(0)
	} else {
		deployment.Spec.Replicas = int32Ptr(1)
	}
	return deployment
}

func (pm *PipelineManager) updatePipelineStatus(origin *api.DrcPipeline, deployment *appsv1.Deployment) (*api.DrcPipeline, error) {
	// NEVER modify objects from the store. It's a read-only, local cache.
	// You can use DeepCopy() to make a deep copy of original object and modify this copy
	// Or create a copy manually for better performance
	pipeline := origin.DeepCopy()

	status := api.DrcPipelineStatus{
		ObservedGeneration: pipeline.Generation,
		Task:               pipeline.Spec.Task,
		Position:           pipeline.Status.Position,
	}

	for i := range pipeline.Status.Conditions {
		status.Conditions = append(status.Conditions, pipeline.Status.Conditions[i])
	}

	var runningCond = api.PipelineCondition{
		Type:               api.PipelineConditionRunning,
		LastUpdateTime:     metav1.Now(),
		LastTransitionTime: metav1.Now(),
	}
	if deployment.Status.AvailableReplicas < 1 {
		runningCond.Status = corev1.ConditionFalse
		if !pipeline.Spec.Paused {
			runningCond.Reason = "PipelineUnavailable"
			runningCond.Message = "Pipeline has no available deployment"
		} else {
			runningCond.Reason = "PipelinePaused"
			runningCond.Message = "Pipeline paused"
		}
	} else {
		runningCond.Status = corev1.ConditionTrue
		runningCond.Reason = "PipelineAvailable"
		runningCond.Message = "Pipeline has available deployment"
	}
	setPipelineCondition(&status, runningCond)
	if runningCond.Status == corev1.ConditionFalse {
		if reflect.DeepEqual(pipeline.Status, status) {
			return pipeline, nil
		}
		pipeline.Status = status
		log.Infof("[updatePipelineStatus] namespace: %v, status: %v", pipeline.Namespace, pipeline.Status)
		return pm.pipeclientset.DrcV1alpha1().DrcPipelines(pipeline.Namespace).UpdateStatus(pipeline)
	}

	selector, err := metav1.LabelSelectorAsSelector(deployment.Spec.Selector)
	if err != nil {
		return nil, err
	}
	pods, err := pm.podLister.Pods(pipeline.Namespace).List(selector)
	if err != nil {
		return nil, errors.Annotatef(err, "fail to list pod by deployment %s", deployment.Name)
	}

	if len(pods) != 1 {
		return nil, errors.Errorf("expect 1 pod for deployment %s, actually %d", deployment.Name, len(pods))
	}

	pod := pods[0]

	if !isPodReady(pod) {
		return nil, errors.Errorf("pod %s not ready", pod.Name)
	}

	status.PodName = pod.Name
	status.Image = pod.Spec.Containers[0].Image
	status.Command = pod.Spec.Containers[0].Command

	url := fmt.Sprintf("http://%s:%d/status", pod.Status.PodIP, pod.Spec.Containers[0].Ports[0].ContainerPort)
	reportStatus, err := getReportStatus(url)
	if err != nil {
		return nil, err
	}

	if reportStatus.Name != pipeline.Name {
		return nil, errors.Errorf("expect name %s, actual %s", pipeline.Name, reportStatus.Name)
	}

	stageCond := api.PipelineCondition{
		Type:               api.PipelineConditionIncremental,
		LastUpdateTime:     metav1.Now(),
		LastTransitionTime: metav1.Now(),
	}
	switch reportStatus.Stage {
	case core.ReportStageIncremental:
		stageCond.Status = corev1.ConditionTrue
		runningCond.Reason = "IncrementalStage"
		runningCond.Message = "Pipeline in incremental stage"
	case core.ReportStageFull:
		stageCond.Status = corev1.ConditionFalse
		runningCond.Reason = "FullStage"
		runningCond.Message = "Pipeline in full stage"
	default:
		return nil, errors.Errorf("unknown report stage %s", reportStatus.Stage)
	}
	setPipelineCondition(&status, stageCond)

	status.ConfigHash = reportStatus.ConfigHash
	status.Position = reportStatus.Position

	if reflect.DeepEqual(pipeline.Status, status) {
		log.Infof("[updatePipelineStatus] identical status, ignore update. %#v", status)
		return pipeline, nil
	}

	pipeline.Status = status
	log.Debug("[updatePipelineStatus] namespace: %v, status: %v", pipeline.Namespace, pipeline.Status)

	k8Version, err := pm.kubeclientset.Discovery().ServerVersion()
	if err != nil {
		return nil, errors.Trace(err)
	}

	if k8Version.Major <= "1" && k8Version.Minor <= "10" {
		p, err := pm.pipeclientset.DrcV1alpha1().DrcPipelines(pipeline.Namespace).Update(pipeline)
		if err != nil {
			return nil, errors.Trace(err)
		}
		return p, nil
	} else {
		p, err := pm.pipeclientset.DrcV1alpha1().DrcPipelines(pipeline.Namespace).UpdateStatus(pipeline)
		if err != nil {
			return nil, errors.Trace(err)
		}
		return p, nil
	}
}

func isPodReady(pod *corev1.Pod) bool {
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.PodReady {
			if cond.Status == corev1.ConditionTrue {
				return true
			}
		}
	}
	return false
}

func getReportStatus(url string) (*core.TaskReportStatus, error) {
	resp, err := http.Get(url)
	if err != nil {
		return nil, errors.Annotatef(err, "fail to get report status, url: %s", url)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Annotatef(err, "fail to read report status, url: %s", url)
	}

	reportStatus := &core.TaskReportStatus{}
	err = json.Unmarshal(body, reportStatus)
	if err != nil {
		return nil, errors.Annotatef(err, "fail to unmarshal task report status. url: %s, body: %s", url, string(body))
	}

	return reportStatus, nil
}

// enqueuePipeline takes a Pipeline resource and converts it into a namespace/name
// string which is then put onto the work queue. This method should *not* be
// passed resources of any type other than Pipeline.
func (pm *PipelineManager) enqueuePipeline(obj *api.DrcPipeline) {
	var key string
	var err error
	if key, err = cache.DeletionHandlingMetaNamespaceKeyFunc(obj); err != nil {
		utilruntime.HandleError(err)
		return
	}
	pm.workqueue.AddRateLimited(key)
}

func (pm *PipelineManager) enqueuePipelineAfter(obj *api.DrcPipeline, duration time.Duration) {
	var key string
	var err error
	if key, err = cache.DeletionHandlingMetaNamespaceKeyFunc(obj); err != nil {
		utilruntime.HandleError(err)
		return
	}
	pm.workqueue.AddAfter(key, duration)
}

// handleObject will take any resource implementing metav1.Object and attempt
// to find the Pipeline resource that 'owns' it. It does this by looking at the
// objects metadata.ownerReferences field for an appropriate OwnerReference.
// It then enqueues that Pipeline resource to be processed. If the object does not
// have an appropriate OwnerReference, it will simply be skipped.
func (pm *PipelineManager) handleObject(obj interface{}) {
	var object metav1.Object
	var ok bool
	if object, ok = obj.(metav1.Object); !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("error decoding object, invalid type"))
			return
		}
		object, ok = tombstone.Obj.(metav1.Object)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("error decoding object tombstone, invalid type"))
			return
		}
		log.Infof("Recovered deleted object '%s' from tombstone", object.GetName())
	}
	if ownerRef := metav1.GetControllerOf(object); ownerRef != nil {
		// If this object is not owned by a Pipeline, we should not do anything more
		// with it.
		if ownerRef.Kind != api.PipelineResourceKind {
			return
		}

		pipeline, err := pm.pipelinesLister.DrcPipelines(object.GetNamespace()).Get(ownerRef.Name)
		if err != nil {
			log.Infof("ignoring orphaned object '%s' of pipeline '%s'", object.GetSelfLink(), ownerRef.Name)
			return
		}

		log.Infof("[PipelineManager.handleObject] enqueue pipeline %s due to %s", pipeline.Name, object.GetSelfLink())
		pm.enqueuePipeline(pipeline)
		return
	}
}

func (pm *PipelineManager) Reset(pipeline *api.DrcPipeline) error {
	pods, err := pm.podLister.Pods(pipeline.Namespace).List(labels.SelectorFromSet(pipeLabels(pipeline)))
	if err != nil {
		return errors.Annotatef(err, "fail to list pod for pipeline %s", pipeline.Name)
	}

	var readyPods []*corev1.Pod
	for _, pod := range pods {
		if isPodReady(pod) {
			readyPods = append(readyPods, pod)
		}
	}

	for _, pod := range readyPods {
		url := fmt.Sprintf("http://%s:%d/reset", pod.Status.PodIP, pod.Spec.Containers[0].Ports[0].ContainerPort)
		resp, err := http.Post(url, "application/json", nil)
		if err != nil {
			return errors.Annotatef(err, "fail to request %s", url)
		}

		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return errors.Annotatef(err, "fail to read body of %s", url)
		}

		if resp.StatusCode != 200 {
			return errors.Annotatef(err, "fail to request %s, response is %s", url, string(body))
		}

		log.Infof("reset full succeed for %s, response %s", pod.Name, string(body))
	}

	return nil
}

func (pm *PipelineManager) needSync(old *api.DrcPipeline, newP *api.DrcPipeline) bool {
	if !reflect.DeepEqual(newP.Status.Task, newP.Spec.Task) {
		return true
	}

	if !reflect.DeepEqual(old.Spec, newP.Spec) {
		return true
	}

	return false
}

func int32Ptr(i int32) *int32 { return &i }

func setPipelineCondition(status *api.DrcPipelineStatus, condition api.PipelineCondition) {
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
