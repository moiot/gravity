package main

import (
	"context"
	"encoding/json"
	"flag"
	"io"
	"os"
	"strings"

	etcd "github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	log "github.com/sirupsen/logrus"

	"github.com/moiot/gravity/pkg/utils"
)

// TargetGroup is the target group read by Prometheus.
type TargetGroup struct {
	Targets []string          `json:"targets,omitempty"`
	Labels  map[string]string `json:"labels,omitempty"`
}

type (
	instances map[string]string
	services  map[string]instances
)

var (
	servicesPrefix = flag.String("prefix", "/services", "etcd path prefix")
	etcdServer     = flag.String("server", "http://127.0.0.1:2379", "etcd server to connect to")
	targetFile     = flag.String("target-file", "tgroups.json", "the file that contains the target groups")
)

func main() {
	flag.Parse()

	client, err := utils.CreateEtcdClient(*etcdServer)
	if err != nil {
		log.Fatalf("Error on connect etcd: %s", err)
	}

	var (
		srvs = services{}
	)
	// Perform an initial read of all services.
	res, err := client.Get(context.TODO(), *servicesPrefix, etcd.WithPrefix())
	if err != nil {
		log.Fatalf("Error on initial retrieval: %s", err)
	}
	for _, kv := range res.Kvs {
		srvs.handle(kv, srvs.update)
	}
	srvs.persist()

	updates := client.Watch(context.TODO(), *servicesPrefix, etcd.WithPrefix())

	// Apply updates sent on the channel.
	for resp := range updates {
		for _, res := range resp.Events {
			log.Infoln(res.Type, string(res.Kv.Key), string(res.Kv.Value))

			h := srvs.update
			if res.Type == mvccpb.DELETE {
				h = srvs.delete
			}
			srvs.handle(res.Kv, h)
		}
		srvs.persist()
	}
}

// handle recursively applies the handler h to the nodes in the subtree
// represented by node.
func (srvs services) handle(node *mvccpb.KeyValue, h func(*mvccpb.KeyValue)) {
	h(node)
}

// update the services based on the given node.
func (srvs services) update(kv *mvccpb.KeyValue) {
	service, instance, url := convert(kv)

	insts, ok := srvs[service]
	if !ok {
		insts = instances{}
	}
	insts[instance] = url
	srvs[service] = insts
}

func convert(kv *mvccpb.KeyValue) (service, instance, url string) {
	sp := strings.Split(string(kv.Key), "/")
	return sp[2], sp[3], string(kv.Value)
}

// delete services or instances based on the given node.
func (srvs services) delete(kv *mvccpb.KeyValue) {
	service, instance, _ := convert(kv)

	// Delete the instance from the service.
	delete(srvs[service], instance)

	if len(srvs[service]) == 0 {
		delete(srvs, service)
	}
}

// persist writes the current services to disc.
func (srvs services) persist() {
	var tgroups []*TargetGroup
	// Write files for current services.
	for job, instances := range srvs {
		var targets []string
		for _, addr := range instances {
			targets = append(targets, addr)
		}

		tgroups = append(tgroups, &TargetGroup{
			Targets: targets,
			Labels:  map[string]string{"job": job},
		})
	}

	content, err := json.Marshal(tgroups)
	if err != nil {
		log.Errorln(err)
		return
	}

	f, err := create(*targetFile)
	if err != nil {
		log.Errorln(err)
		return
	}
	defer f.Close()

	if _, err := f.Write(content); err != nil {
		log.Errorln(err)
	}
}

type renameFile struct {
	*os.File
	filename string
}

func (f *renameFile) Close() error {
	f.File.Sync()

	if err := f.File.Close(); err != nil {
		return err
	}
	return os.Rename(f.File.Name(), f.filename)
}

func create(filename string) (io.WriteCloser, error) {
	tmpFilename := filename + ".tmp"

	f, err := os.Create(tmpFilename)
	if err != nil {
		return nil, err
	}

	rf := &renameFile{
		File:     f,
		filename: filename,
	}
	return rf, nil
}
