package gatekeeper

import (
	"math/rand"
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

var (
	killed = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "drc_v2",
		Subsystem: "gatekeeper",
		Name:      "chaos",
		Help:      "killed component by chaos",
	}, []string{"kind", "name"})
)

type ChaosMonkey struct {
	namespace string

	kube dynamic.Interface

	tickerInterval time.Duration
	closeC         <-chan struct{}
	wg             sync.WaitGroup
}

func NewChaosMonkey(namespace string, kube dynamic.Interface, duration string, stopCh <-chan struct{}) *ChaosMonkey {
	prometheus.MustRegister(killed)
	monkey := &ChaosMonkey{
		namespace: namespace,
		kube:      kube,
		closeC:    stopCh,
	}
	tickerInterval, err := time.ParseDuration(duration)
	if err != nil {
		log.Fatalf("wrong duration format %s", duration)
	}
	monkey.tickerInterval = tickerInterval
	return monkey
}

func (c *ChaosMonkey) Start() error {
	go c.run()
	return nil
}

func (c *ChaosMonkey) run() {
	ticker := time.NewTicker(c.tickerInterval)
	defer ticker.Stop()

	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))

	m := map[string]string{
		"app.kubernetes.io/name": "drc",
	}
	selector := labels.SelectorFromSet(m)

	podGvr := schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"}
	deploymentGvr := schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"}

	for {
		select {
		case <-c.closeC:
			return

		case <-ticker.C:
			if rnd.Float32() < 0.5 {
				continue
			}

			gvr := podGvr
			if rnd.Float32() < 0.3 {
				gvr = deploymentGvr
			}

			list, err := c.kube.Resource(gvr).Namespace(c.namespace).List(metav1.ListOptions{
				LabelSelector: selector.String(),
			})
			if err != nil {
				log.Error(errors.Annotatef(err, "[ChaosMonkey] fail to list %s", gvr.Resource))
				continue
			}

			if len(list.Items) == 0 {
				log.Infof("[ChaosMonkey] no %s to kill", gvr.String())
				continue
			}

			item := list.Items[rnd.Intn(len(list.Items))]
			err = c.kube.Resource(gvr).Namespace(item.GetNamespace()).Delete(item.GetName(), deleteOps(rnd))
			if err != nil {
				log.Error(errors.Annotatef(err, "[ChaosMonkey] fail to delete %s %s", item.GetKind(), item.GetName()))
			} else {
				killed.WithLabelValues(item.GetKind(), item.GetLabels()["app.kubernetes.io/instance"]).Add(1)
				log.Infof("[ChaosMonkey] deleted %s %s", item.GetKind(), item.GetLabels()["app.kubernetes.io/instance"])
			}
		}
	}
}

func deleteOps(rnd *rand.Rand) *metav1.DeleteOptions {
	ops := metav1.NewDeleteOptions(int64(rnd.Intn(8)))
	return ops
}

func (c *ChaosMonkey) Close() {
	c.wg.Wait()
}
