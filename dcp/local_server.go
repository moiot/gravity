package dcp

import (
	_ "github.com/go-sql-driver/mysql"
	"github.com/moiot/gravity/dcp/barrier"
	"github.com/moiot/gravity/dcp/checker"
	"github.com/moiot/gravity/dcp/collector"
	"github.com/moiot/gravity/pkg/protocol/dcp"

	log "github.com/sirupsen/logrus"
)

// StartLocal starts collector, checker & barrier all in one process
func StartLocal(barrierConfig *barrier.Config, collectorConfigs []collector.Config, checkerConfig *checker.Config, shutdown chan struct{}, alarm chan<- checker.Result) error {
	barrier.Start(barrierConfig, shutdown)

	collectors := make([]collector.Interface, 0, len(collectorConfigs))
	allMsg := make(chan *dcp.Message, 100)
	for _, cfg := range collectorConfigs {
		var c collector.Interface
		switch cfg := cfg.(type) {
		case *collector.MysqlConfig:
			c = collector.NewMysqlCollector(cfg)
		case *collector.GrpcConfig:
			c = collector.NewGrpc(cfg)
		}
		c.Start()
		go func(c <-chan *dcp.Message) {
			for m := range c {
				allMsg <- m
			}
		}(c.GetChan())
		collectors = append(collectors, c)
	}

	c := checker.New(checkerConfig)

	alarm <- &checker.Ready{}

loop:
	for {
		select {
		case <-shutdown:
			log.Info("local server receive shutdown")
			break loop

		case r := <-c.ResultChan:
			alarm <- r

		case m := <-allMsg:
			c.Consume(m)
		}
	}

	for _, c := range collectors {
		c.Stop()
	}
	close(allMsg)
	c.Shutdown()

	return nil
}
