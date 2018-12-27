package main

import (
	"flag"
	"os"
	"time"

	log "github.com/sirupsen/logrus"

	kubeinformers "k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/moiot/gravity/k8s_operator/apiserver"
	"github.com/moiot/gravity/k8s_operator/controller"
	clusterclient "github.com/moiot/gravity/k8s_operator/pkg/client/cluster/clientset/versioned"
	clusterinformer "github.com/moiot/gravity/k8s_operator/pkg/client/cluster/informers/externalversions"
	pipeclient "github.com/moiot/gravity/k8s_operator/pkg/client/pipeline/clientset/versioned"
	pipeinformer "github.com/moiot/gravity/k8s_operator/pkg/client/pipeline/informers/externalversions"
	"github.com/moiot/gravity/k8s_operator/pkg/signals"
)

var (
	masterURL  string
	kubeconfig string
	port       int
)

func main() {
	flag.Parse()

	namespace := os.Getenv("MY_POD_NAMESPACE")
	if namespace == "" {
		namespace = "default"
	}

	stopCh := signals.SetupSignalHandler()

	cfg, err := clientcmd.BuildConfigFromFlags(masterURL, kubeconfig)
	if err != nil {
		log.Fatalf("Error building kubeconfig: %s", err.Error())
	}

	kubeClient, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		log.Fatalf("Error building kubernetes clientset: %s", err.Error())
	}

	clusterClient, err := clusterclient.NewForConfig(cfg)
	if err != nil {
		log.Fatalf("Error building cluster client: %s", err.Error())
	}

	pipelineClient, err := pipeclient.NewForConfig(cfg)
	if err != nil {
		log.Fatalf("Error building pipeline client: %s", err.Error())
	}

	clusterInformerFactory := clusterinformer.NewSharedInformerFactoryWithOptions(clusterClient, time.Second*30, clusterinformer.WithNamespace(namespace))
	pipelinesInformerFactory := pipeinformer.NewSharedInformerFactoryWithOptions(pipelineClient, time.Second*30, pipeinformer.WithNamespace(namespace))
	kubeInformerFactory := kubeinformers.NewSharedInformerFactoryWithOptions(kubeClient, time.Second*30, kubeinformers.WithNamespace(namespace))

	clusterController := controller.NewClusterController(
		namespace,
		kubeInformerFactory,
		kubeClient,
		clusterClient,
		clusterInformerFactory.Drc().V1alpha1().DrcClusters(),
		pipelineClient,
		pipelinesInformerFactory.Drc().V1alpha1().DrcPipelines(),
	)

	go kubeInformerFactory.Start(stopCh)
	go clusterInformerFactory.Start(stopCh)
	go pipelinesInformerFactory.Start(stopCh)

	log.Infof("running controllers")

	if err := clusterController.Run(5, stopCh); err != nil {
		log.Fatalf("Error running clusterController: %v", err.Error())
	}

	apiServer := apiserver.NewApiServer(namespace, kubeClient, pipelineClient, clusterController, port)
	apiServer.Start()

	<-stopCh
	log.Info("Shutting down operator")
}

func init() {
	flag.StringVar(&kubeconfig, "kubeconfig", "", "Path to a kubeconfig. Only required if out-of-cluster.")
	flag.StringVar(&masterURL, "master", "", "The address of the Kubernetes API server. Overrides any value in kubeconfig. Only required if out-of-cluster.")
	flag.IntVar(&port, "p", 8080, "port number")
}
