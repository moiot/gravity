package utils

import (
	"strings"
	"time"

	"github.com/coreos/etcd/clientv3"
	log "github.com/sirupsen/logrus"
)

const (
	dialTimeout = 5 * time.Second
)

func MustCreateEtcdClient(etcdEndpoints string) *clientv3.Client {
	etcdClient, err := CreateEtcdClient(etcdEndpoints)
	if err != nil {
		log.Fatalf("[gravity] failed to contact etcd: %v", err)
	}
	return etcdClient
}

func CreateEtcdClient(etcdEndpoints string) (*clientv3.Client, error) {
	endpoints := strings.Split(etcdEndpoints, ",")
	return clientv3.New(clientv3.Config{
		Endpoints: endpoints,
		// Etcd client instance needs to specify a value for DialTimeout to avoid permanent block
		// when it encountered an abnormality
		DialTimeout: dialTimeout,
	})
}
