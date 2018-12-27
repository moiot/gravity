package collector_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gstruct"

	"database/sql"

	"github.com/moiot/gravity/dcp/collector"
	"github.com/moiot/gravity/pkg/utils"

	_ "github.com/go-sql-driver/mysql"
	log "github.com/sirupsen/logrus"

	"github.com/moiot/gravity/dcp/barrier"
	"github.com/moiot/gravity/pkg/protocol/dcp"
)

var _ = PDescribe("Collector Mysql", func() {

	var (
		db *sql.DB
	)

	dbConfig := utils.TestConfig()

	BeforeEach(func() {
		var err error
		db, err = utils.CreateDBConnection(dbConfig)
		Expect(err).ShouldNot(HaveOccurred())

		_, err = db.Exec("create database if not exists collector_test")
		Expect(err).ShouldNot(HaveOccurred())

		_, err = db.Exec(`
CREATE TABLE if not EXISTS collector_test.test (
  id int(11) unsigned NOT NULL AUTO_INCREMENT,
  v bigint(11) unsigned NOT NULL,
  dt datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  ts timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;`)
		Expect(err).ShouldNot(HaveOccurred())

		_, err = db.Exec("truncate table collector_test.test")
		Expect(err).ShouldNot(HaveOccurred())

		_, err = db.Exec("reset master")
		Expect(err).ShouldNot(HaveOccurred())

	})

	AfterEach(func() {
		if db != nil {
			db.Exec("drop database collector_test")
			db.Exec("reset master")
			db.Close()
		}
	})

	It("Should work for CURD", func() {
		shutdown := make(chan struct{})
		barrier.Start(&barrier.Config{
			TestDB:        db,
			TickerSeconds: 1,
		}, shutdown)
		close(shutdown)

		config := collector.MysqlConfig{
			Db: collector.DbConfig{
				Name:     "test",
				Host:     dbConfig.Host,
				Port:     uint(dbConfig.Port),
				Username: dbConfig.Username,
				Password: dbConfig.Password,
				ServerId: 998,
			},
			TagConfigs: []collector.TagConfig{
				{
					Tag: "src",
					Tables: []collector.SchemaAndTable{
						{
							Schema:        "collector_test",
							Table:         "test",
							PrimaryKeyIdx: 0,
						},
					},
				},
			},
		}

		c := collector.NewMysqlCollector(&config)
		c.Start()

		_, err := db.Exec("insert into collector_test.test(id, v) values (1, 1)")
		Expect(err).ShouldNot(HaveOccurred())

		_, err = db.Exec("update collector_test.test set v = 2 where id = 1")
		Expect(err).ShouldNot(HaveOccurred())

		_, err = db.Exec("delete from collector_test.test where id = 1")
		Expect(err).ShouldNot(HaveOccurred())

		var m *dcp.Message

		m = <-c.GetChan()
		log.Info(m)
		Expect(m.GetBody()).Should(BeAssignableToTypeOf(&dcp.Message_Barrier{}))

		for t := range c.GetChan() {
			if _, ok := t.Body.(*dcp.Message_Payload); ok {
				m = t
				break
			}
		}
		Expect(*m.GetPayload()).Should(MatchFields(IgnoreExtras|IgnoreMissing, Fields{
			"Id":      Equal("1"),
			"Content": HavePrefix("INSERT"),
			// Ignore lack of "C" in the matcher.
		}))

		m = <-c.GetChan()
		Expect(m.GetBody()).Should(BeAssignableToTypeOf(&dcp.Message_Payload{}))
		Expect(*m.GetPayload()).Should(MatchFields(IgnoreExtras|IgnoreMissing, Fields{
			"Id":      Equal("1"),
			"Content": HavePrefix("UPDATE"),
			// Ignore lack of "C" in the matcher.
		}))

		m = <-c.GetChan()
		Expect(m.GetBody()).Should(BeAssignableToTypeOf(&dcp.Message_Payload{}))
		Expect(*m.GetPayload()).Should(MatchFields(IgnoreExtras|IgnoreMissing, Fields{
			"Id":      Equal("1"),
			"Content": HavePrefix("DELETE"),
			// Ignore lack of "C" in the matcher.
		}))

		c.Stop()

		Eventually(c.GetChan()).Should(BeClosed())
	})
})
