package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/moiot/gravity/dcp"
	"github.com/moiot/gravity/dcp/barrier"
	"github.com/moiot/gravity/dcp/checker"
	"github.com/moiot/gravity/dcp/collector"
	"github.com/moiot/gravity/pkg/utils"
	log "github.com/sirupsen/logrus"
)

func main() {
	dbConfig := utils.TestConfig()

	barrierConfig := barrier.Config{
		Db:            *dbConfig,
		TickerSeconds: 2,
	}

	collectorConfigs := []collector.Config{
		&collector.MysqlConfig{
			Db: collector.DbConfig{
				Name:     "db",
				Host:     dbConfig.Host,
				Port:     uint(dbConfig.Port),
				Username: dbConfig.Username,
				Password: dbConfig.Password,
				ServerId: 999,
			},

			TagConfigs: []collector.TagConfig{
				{
					Tag: "src",
					Tables: []collector.SchemaAndTable{
						{

							Schema:        "drc",
							Table:         "src",
							PrimaryKeyIdx: 0,
						},
					},
				},
				{
					Tag: "target",
					Tables: []collector.SchemaAndTable{
						{

							Schema:        "drc",
							Table:         "target",
							PrimaryKeyIdx: 0,
						},
					},
				},
			},
		},
	}

	checkerConfig := checker.Config{
		SourceTag:      "src",
		TargetTags:     []string{"target"},
		TimeoutSeconds: 2,
	}

	shutDown := make(chan struct{})
	alarm := make(chan checker.Result, 10)
	closed := make(chan error)

	go func() {
		closed <- dcp.StartLocal(&barrierConfig, collectorConfigs, &checkerConfig, shutDown, alarm)
	}()

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

loop:
	for {
		select {
		case a := <-alarm:
			fmt.Printf("%T\n", a)

		case sig := <-sc:
			log.Info(sig)
			close(shutDown)

		case err := <-closed:
			if err != nil {
				log.Error(err)
			}
			break loop
		}
	}
}
