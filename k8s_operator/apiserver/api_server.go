package apiserver

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/pprof"
	"net/url"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	log "github.com/sirupsen/logrus"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/moiot/gravity/k8s_operator/controller"
	clusterapi "github.com/moiot/gravity/k8s_operator/pkg/apis/cluster/v1alpha1"
	pipeapi "github.com/moiot/gravity/k8s_operator/pkg/apis/pipeline/v1alpha1"
	client "github.com/moiot/gravity/k8s_operator/pkg/client/pipeline/clientset/versioned"
)

type ApiServer struct {
	srv           *http.Server
	kubeclientset kubernetes.Interface
	pipeclientset client.Interface
	controller    controller.Interface
	namespace     string
}

func (s *ApiServer) ServeHTTP(resp http.ResponseWriter, req *http.Request) {
	s.srv.Handler.ServeHTTP(resp, req)
}

func NewApiServer(
	namespace string,
	kubeclientset kubernetes.Interface,
	pipeclientset client.Interface,
	controller controller.Interface,
	port int) *ApiServer {

	gin.SetMode(gin.ReleaseMode)
	router := gin.Default()

	srv := &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: router,
	}
	apiServer := &ApiServer{
		namespace:     namespace,
		kubeclientset: kubeclientset,
		pipeclientset: pipeclientset,
		controller:    controller,
		srv:           srv,
	}

	router.Any("/metrics", gin.WrapH(promhttp.Handler()))

	router.GET("/debug/pprof/", gin.WrapF(pprof.Index))
	router.GET("/debug/pprof/cmdline", gin.WrapF(pprof.Cmdline))
	router.GET("/debug/pprof/profile", gin.WrapF(pprof.Profile))
	router.GET("/debug/pprof/symbol", gin.WrapF(pprof.Symbol))
	router.GET("/debug/pprof/trace", gin.WrapF(pprof.Trace))
	router.GET("/debug/pprof/block", gin.WrapH(pprof.Handler("block")))
	router.GET("/debug/pprof/goroutine", gin.WrapH(pprof.Handler("goroutine")))
	router.GET("/debug/pprof/heap", gin.WrapH(pprof.Handler("heap")))
	router.GET("/debug/pprof/mutex", gin.WrapH(pprof.Handler("mutex")))
	router.GET("/debug/pprof/threadcreate", gin.WrapH(pprof.Handler("threadcreate")))

	router.POST("/pipeline", apiServer.createPipe)
	router.PUT("/pipeline/:name", apiServer.updatePipe)
	router.GET("/pipeline/:name", apiServer.getPipe)
	router.GET("/pipeline", apiServer.listPipe)
	router.POST("/pipeline/:name/reset", apiServer.reset)
	router.DELETE("/pipeline/:name", apiServer.deletePipe)

	return apiServer
}

func (s *ApiServer) Start() {
	go func() {
		if err := s.srv.ListenAndServe(); err != nil {
			log.Infof("[ApiServer] closed with %s", err)
		}
	}()
	log.Info("[ApiServer] Started")
}

func (s *ApiServer) Stop() {
	log.Info("[ApiServer] Stopping")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.srv.Shutdown(ctx); err != nil {
		log.Error("[ApiServer] error: ", err)
	}
	log.Info("[ApiServer] Stopped")
}

func (s *ApiServer) createPipe(c *gin.Context) {
	var request ApiPipeline
	if err := c.BindJSON(&request); err != nil {
		log.Errorf("[ApiServer.createPipe] bind json error. %s", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": err})
		return
	}

	if err := request.validate(); err != nil {
		log.Errorf("[ApiServer.createPipe] error Validate task spec: %s. %#v", err, request.Spec)
		c.JSON(http.StatusBadRequest, gin.H{"error": "error Validate task spec: " + err.Error()})
		return
	}

	pipeline := request.toK8()
	pipeline.OwnerReferences = []metav1.OwnerReference{
		*metav1.NewControllerRef(s.controller.GetCluster(), pipeapi.SchemeGroupVersion.WithKind(clusterapi.ClusterResourceKind)),
	}
	cluster := s.controller.GetCluster()
	rule := cluster.FindDeploymentRule(pipeline.Name)
	if rule == nil {
		log.Errorf("[ApiServer.createPipe] can't find deployment rule for pipeline %s", request.Name)
		c.JSON(http.StatusBadRequest, gin.H{"error": "can't find deployment rule"})
		return
	}
	pipeline.Spec.Image = rule.Image
	pipeline.Spec.Command = rule.Command

	pipeline, err := s.pipeclientset.DrcV1alpha1().DrcPipelines(s.namespace).Create(pipeline)
	if err != nil {
		if apierrors.IsAlreadyExists(err) {
			log.Errorf("[ApiServer.createPipe] error duplicate name %s. err: %s", request.Name, err)
			c.JSON(http.StatusBadRequest, gin.H{"error": "pipeline " + request.Name + " already exists"})
			return
		}
		log.Errorf("[ApiServer.createPipe] error create pipeline. %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err})
		return
	}

	config := request.newConfigMap(pipeline)
	_, err = s.kubeclientset.CoreV1().ConfigMaps(s.namespace).Create(config)
	if err != nil {
		s.pipeclientset.DrcV1alpha1().DrcPipelines(pipeline.Namespace).Delete(pipeline.Name, &metav1.DeleteOptions{})
		log.Errorf("[ApiServer.createPipe] error create config. %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err})
		return
	}

	log.Infof("[ApiServer.createPipe] successfully created %s", pipeline.Name)
	c.Header("Location", url.PathEscape(fmt.Sprintf("/pipeline/%s", pipeline.Name)))
	c.JSON(http.StatusCreated, gin.H{"pipelineId": pipeline.UID, "name": pipeline.Name, "msg": fmt.Sprintf("created pipeline %s", pipeline.Name)})
}

func (s *ApiServer) updatePipe(c *gin.Context) {
	request := &ApiPipeline{}
	if err := c.BindJSON(&request); err != nil {
		log.Errorf("[ApiServer.updatePipe] fail parse request. %s", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": "failed to parse parameter: " + err.Error()})
		return
	}

	if err := request.validate(); err != nil {
		log.Errorf("[ApiServer.updatePipe] error Validate task spec: %s. %#v", err, request.Spec)
		c.JSON(http.StatusBadRequest, gin.H{"error": "error Validate task spec: " + err.Error()})
		return
	}

	name := c.Param("name")
	pipeline, err := s.controller.GetK8Pipeline(s.namespace, name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			c.JSON(http.StatusNotFound, gin.H{"error": "no pipeline found for " + c.Param("name")})
			return
		} else {
			log.Errorf("[ApiServer.updatePipe] error get pipeline. %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": err})
			return
		}
	}

	if pipeline.Name != request.Name {
		c.JSON(http.StatusBadRequest, gin.H{"error": "pipeline name is unmodifiable"})
		return
	}

	newPipeline := request.toK8()
	newPipeline, err = s.pipeclientset.DrcV1alpha1().DrcPipelines(s.namespace).Update(newPipeline)
	if err != nil {
		if apierrors.IsConflict(err) {
			log.Errorf("[ApiServer.updatePipe] pipeline %s has been updated. err: %s", request.Name, err)
			c.JSON(http.StatusConflict, gin.H{"error": "pipeline has been updated. please retry. "})
			return
		}
		log.Errorf("[ApiServer.updatePipe] error update %s. %v", newPipeline, err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err})
		return
	}

	if pipeline.Spec.ConfigHash != newPipeline.Spec.ConfigHash {
		configMap := request.newConfigMap(newPipeline)
		_, err := s.kubeclientset.CoreV1().ConfigMaps(s.namespace).Update(configMap)
		if err != nil {
			log.Errorf("[ApiServer.updatePipe] error update config. %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": err})
			return
		}
	}

	c.JSON(http.StatusOK, gin.H{"msg": "update success"})
}

func (s *ApiServer) getPipe(c *gin.Context) {
	name := c.Param("name")
	pipeline, err := s.controller.GetK8Pipeline(s.namespace, name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			c.JSON(http.StatusNotFound, gin.H{"error": "no pipeline found for " + c.Param("name")})
			return
		} else {
			log.Errorf("[ApiServer.getPipe] error get pipeline. %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": err})
			return
		}
	}

	ret := &ApiPipeline{}
	ret.fromK8(pipeline)

	configMaps, err := s.kubeclientset.CoreV1().ConfigMaps(s.namespace).Get(name, metav1.GetOptions{})
	if err != nil {
		log.Errorf("[ApiServer.getPipe] error get config maps. %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err})
		return
	}
	raw := json.RawMessage(configMaps.Data[pipeapi.ConfigFileKey])
	ret.Spec.Config = &raw
	c.JSON(http.StatusOK, ret)
}

func (s *ApiServer) reset(c *gin.Context) {
	name := c.Param("name")
	pipeline, err := s.controller.GetK8Pipeline(s.namespace, name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			c.JSON(http.StatusNotFound, gin.H{"error": "no pipeline found for " + c.Param("name")})
			return
		} else {
			log.Errorf("[ApiServer.reset] error get pipeline. %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": err})
			return
		}
	}

	if err := s.controller.Reset(pipeline); err != nil {
		log.Errorf("[ApiServer.reset] error reset. %s", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err})
		return
	}
	c.JSON(http.StatusOK, gin.H{"msg": "succeed"})
}

func (s *ApiServer) listPipe(c *gin.Context) {
	pipelineList, err := s.controller.ListK8Pipelines(s.namespace)
	if err != nil {
		log.Errorf("[ApiServer.listPipe] error list pipeline. %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err})
		return
	}

	ret := make([]ApiPipeline, len(pipelineList))
	for i, p := range pipelineList {
		vo := &ApiPipeline{}
		vo.fromK8(p)
		ret[i] = *vo
	}
	c.JSON(http.StatusOK, ret)
}

func (s *ApiServer) deletePipe(c *gin.Context) {
	err := s.pipeclientset.DrcV1alpha1().DrcPipelines(s.namespace).Delete(c.Param("name"), &metav1.DeleteOptions{})
	if err != nil {
		log.Errorf("[ApiServer.deletePipe] %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err})
		return
	}
	c.JSON(http.StatusOK, gin.H{"msg": "deleted"})
}
