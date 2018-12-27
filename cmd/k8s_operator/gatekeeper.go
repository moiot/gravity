package main

import (
	"database/sql"
	"flag"
	_ "net/http/pprof"
	"os"

	_ "github.com/go-sql-driver/mysql"
	log "github.com/sirupsen/logrus"

	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"

	"github.com/moiot/gravity/k8s_operator/gatekeeper"
	clusterclient "github.com/moiot/gravity/k8s_operator/pkg/client/cluster/clientset/versioned"
	"github.com/moiot/gravity/k8s_operator/pkg/signals"
)

func main() {
	var casePath, operatorAddr, sourceMysql, targetMysql, chaosInterval string
	flag.StringVar(&casePath, "f", "/etc/drc/cases.yaml", "path of case config")
	flag.StringVar(&operatorAddr, "api", "http://drc-operator", "url of operator")
	flag.StringVar(&sourceMysql, "srcMySQL", "root@tcp(source-mysql:3306)/?interpolateParams=true&readTimeout=%s&parseTime=true&collation=utf8mb4_general_ci", "source mysql url")
	flag.StringVar(&targetMysql, "tarMySQL", "root@tcp(target-mysql:3306)/?interpolateParams=true&readTimeout=%s&parseTime=true&collation=utf8mb4_general_ci", "target mysql url")
	flag.StringVar(&chaosInterval, "chaosInterval", "45s", "tick interval of chaos monkey")
	flag.Parse()

	namespace := os.Getenv("MY_POD_NAMESPACE")
	if namespace == "" {
		namespace = "default"
	}

	stopCh := signals.SetupSignalHandler()

	cfg, err := rest.InClusterConfig()
	if err != nil {
		log.Fatalf("Error building kubeconfig: %s", err.Error())
	}

	kubeDynamic, err := dynamic.NewForConfig(cfg)
	if err != nil {
		log.Fatalf("Error building kubernetes dynamic: %s", err.Error())
	}

	clusterClient, err := clusterclient.NewForConfig(cfg)
	if err != nil {
		log.Fatalf("Error building cluster client: %s", err.Error())
	}

	chaosMonkey := gatekeeper.NewChaosMonkey(namespace, kubeDynamic, chaosInterval, stopCh)
	if err = chaosMonkey.Start(); err != nil {
		log.Fatalf("fail to start chaos monkey. %s", err.Error())
	}

	sourceDB, err := sql.Open("mysql", sourceMysql)
	if err != nil {
		log.Panic(err)
	}
	defer sourceDB.Close()

	targetDB, err := sql.Open("mysql", targetMysql)
	if err != nil {
		log.Panic(err)
	}
	defer targetDB.Close()

	repo := gatekeeper.NewRepository(casePath, operatorAddr, sourceDB, targetDB, clusterClient.DrcV1alpha1().DrcClusters(namespace))
	api := gatekeeper.NewApiServer(repo)
	api.Start()

	<-stopCh

	log.Info("Shutting down operator")

	chaosMonkey.Close()
	api.Stop()
}
