package dcp_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"database/sql"

	"github.com/moiot/gravity/dcp"
	"github.com/moiot/gravity/dcp/checker"
	"github.com/moiot/gravity/pkg/utils"

	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/moiot/gravity/dcp/barrier"
	"github.com/moiot/gravity/dcp/collector"
)

const dcpTestDB = "dcp_test"

var _ = PDescribe("Local server", func() {
	dbConfig := utils.TestConfig()
	var db *sql.DB
	var shutDown chan struct{}
	var alarm chan checker.Result
	var closed chan error

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

							Schema:        dcpTestDB,
							Table:         "src",
							PrimaryKeyIdx: 0,
						},
					},
				},
				{
					Tag: "target",
					Tables: []collector.SchemaAndTable{
						{

							Schema:        dcpTestDB,
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

	waitTime := checkerConfig.TimeoutSeconds + barrierConfig.TickerSeconds

	BeforeEach(func() {
		var err error

		shutDown = make(chan struct{})
		alarm = make(chan checker.Result, 10)
		closed = make(chan error)

		db, err = utils.CreateDBConnection(dbConfig)
		Expect(err).ShouldNot(HaveOccurred())

		exec(db, fmt.Sprintf("drop database if exists %s", dcpTestDB))
		exec(db, fmt.Sprintf("create database if not exists %s", dcpTestDB))

		exec(db, fmt.Sprintf(`
CREATE TABLE if not EXISTS %s.src (
  id int(11) unsigned NOT NULL AUTO_INCREMENT,
  v bigint(11) unsigned NOT NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;`, dcpTestDB))

		exec(db, fmt.Sprintf(`
CREATE TABLE if not EXISTS %s.target (
  id int(11) unsigned NOT NULL AUTO_INCREMENT,
  v bigint(11) unsigned NOT NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;`, dcpTestDB))

		exec(db, "reset master")

		go func() {
			closed <- dcp.StartLocal(&barrierConfig, collectorConfigs, &checkerConfig, shutDown, alarm)
		}()
	})

	AfterEach(func() {
		close(shutDown)
		Eventually(closed).Should(Receive(BeNil()))

		if db != nil {
			exec(db, fmt.Sprintf("drop database if exists %s", dcpTestDB))
			exec(db, "reset master")
			db.Close()
		}
	})

	It("Should report Same when sync", func() {
		Expect(<-alarm).Should(BeAssignableToTypeOf(&checker.Ready{}))

		exec(db, fmt.Sprintf("insert into %s.src(id, v) VALUES (1, 0)", dcpTestDB))
		exec(db, fmt.Sprintf("insert into %s.target(id, v) VALUES (1, 0)", dcpTestDB))

		Eventually(alarm, time.Second*time.Duration(waitTime)).Should(Receive(BeAssignableToTypeOf(&checker.Same{})))
	})

	It("Should report Diff on different modification", func() {
		Expect(<-alarm).Should(BeAssignableToTypeOf(&checker.Ready{}))

		exec(db, fmt.Sprintf("insert into %s.src(id, v) VALUES (1, 0)", dcpTestDB))
		exec(db, fmt.Sprintf("insert into %s.target(id, v) VALUES (1, 1)", dcpTestDB))

		Eventually(alarm, time.Second*time.Duration(waitTime)).Should(Receive(BeAssignableToTypeOf(&checker.Diff{})))
	})

	It("Should report Diff when lack of target data", func() { //barrier is still received, Timeout just for no barrier
		Expect(<-alarm).Should(BeAssignableToTypeOf(&checker.Ready{}))

		exec(db, fmt.Sprintf("insert into %s.src(id, v) VALUES (1, 0)", dcpTestDB))

		Eventually(alarm, time.Second*time.Duration(waitTime)).Should(Receive(BeAssignableToTypeOf(&checker.Diff{})))
	})
})

func exec(db *sql.DB, stmt string) {
	_, err := db.Exec(stmt)
	Expect(err).ShouldNot(HaveOccurred(), stmt)
}
