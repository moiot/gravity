package gatekeeper

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/moiot/gravity/k8s_operator/apiserver"

	"github.com/moiot/gravity/gravity/config"

	"github.com/moiot/gravity/pkg/utils"

	"github.com/moiot/gravity/pkg/mysql_test"
)

var inconsistent = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Namespace: "drc_v2",
	Subsystem: "gatekeeper",
	Name:      "inconsistent",
	Help:      "count of inconsistent checks",
}, []string{"case"})

func init() {
	prometheus.MustRegister(inconsistent)
}

type Version struct {
	Name         string
	RunningCases []string

	cases []*TestCase
}

func (v *Version) Reconcile(configs []TestCaseConfig, operatorAddr string, sourceDB *sql.DB, targetDB *sql.DB, bootstrap bool) {
	set := make(map[string]bool)
	for _, c := range configs {
		set[c.Name] = true
	}

	for _, previous := range v.RunningCases {
		if !set[previous] {
			tc := NewTestCase(TestCaseConfig{Name: previous}, v.Name, operatorAddr, sourceDB, targetDB)
			tc.Delete()
		}
	}

	v.RunningCases = v.RunningCases[:0]
	for _, c := range configs {
		v.RunningCases = append(v.RunningCases, c.Name)
		tc := NewTestCase(c, v.Name, operatorAddr, sourceDB, targetDB)
		tc.Prepare()
		v.cases = append(v.cases, tc)
		tc.Start(bootstrap)
	}
}

func (v *Version) Delete() {
	for _, tc := range v.cases {
		tc.Delete()
	}
	v.cases = v.cases[:0]
	v.RunningCases = v.RunningCases[:0]
}

type TestCaseConfig struct {
	Name                       string `json:"name"  yaml:"name"`
	Desc                       string `json:"desc"  yaml:"desc"`
	mysql_test.GeneratorConfig `json:",inline"  yaml:",inline"`
}

type TestCase struct {
	TestCaseConfig

	fullName     string
	operatorAddr string
	sourceDB     *sql.DB
	targetDB     *sql.DB

	client    *http.Client
	generator *mysql_test.Generator

	wg    sync.WaitGroup
	stopC chan struct{}
}

func NewTestCase(c TestCaseConfig, version string, operatorAddr string, sourceDB *sql.DB, targetDB *sql.DB) *TestCase {
	tc := &TestCase{
		TestCaseConfig: c,
		fullName:       fmt.Sprintf("%s-%s", version, c.Name),
		operatorAddr:   operatorAddr,
		sourceDB:       sourceDB,
		targetDB:       targetDB,
		stopC:          make(chan struct{}),
	}

	tc.client = &http.Client{
		Timeout: 5 * time.Second,
	}

	tc.generator = &mysql_test.Generator{
		SourceDB:     tc.sourceDB,
		SourceSchema: tc.fullName,
		TargetDB:     tc.targetDB,
		TargetSchema: tc.fullName,
		GeneratorConfig: mysql_test.GeneratorConfig{
			NrTables:          tc.NrTables,
			NrSeedRows:        tc.NrSeedRows,
			DeleteRatio:       tc.DeleteRatio,
			InsertRatio:       tc.InsertRatio,
			Concurrency:       tc.Concurrency,
			TransactionLength: tc.TransactionLength,
		},
	}

	return tc
}

func (tc *TestCase) Prepare() {
	tc.prepareDB()
	tc.createPipe()
}

func (tc *TestCase) Start(bootstrap bool) {
	tc.wg.Add(1)
	go tc.run(bootstrap)
}

func (tc *TestCase) run(bootstrap bool) {
	defer tc.wg.Done()

	tc.generator.SetupTestTables()

	if bootstrap {
		start := time.Now()
		log.Infof("[TestCase.prepareDB] %s seed %d rows ........", tc.fullName, tc.NrSeedRows)
		tc.generator.SeedRows()
		log.Infof("[TestCase.prepareDB] %s seed %d rows finished, elapsed %s", tc.fullName, tc.NrSeedRows, time.Since(start))
	}

	timeout := 30 * time.Second
	maxTimeout := 10 * time.Minute

	for {
		select {
		case <-tc.stopC:
			return

		default:
			if timeout == maxTimeout || timeout.Seconds()*2 > maxTimeout.Seconds() {
				timeout = maxTimeout
			} else {
				timeout = timeout * 2
			}
			c, cancel := context.WithTimeout(context.Background(), timeout)
			done := tc.generator.ParallelUpdate(c)
			<-c.Done()
			cancel()
			done.Wait()
			if c.Err() != context.DeadlineExceeded {
				log.Infof("[TestCase.Start] exist due to %s", c.Err())
				return
			}

			err := tc.generator.TestChecksum()
			for err != nil {
				inconsistent.WithLabelValues(tc.fullName).Add(1)
				log.Errorf("inconsistent data. case: %s, err: %s", tc.fullName, err)
				select {
				case <-tc.stopC:
					return

				case <-time.After(time.Second):
				}
				err = tc.generator.TestChecksum()
			}
			inconsistent.WithLabelValues(tc.fullName).Set(0)
		}
	}
}

func (tc *TestCase) Delete() {
	close(tc.stopC)
	tc.wg.Wait()
	inconsistent.WithLabelValues(tc.fullName).Set(0)

	tc.deletePipe()
	tc.dropDB()
}

func (tc *TestCase) createPipe() {
	url := fmt.Sprintf("%s/pipeline/%s", tc.operatorAddr, tc.fullName)
	resp, err := tc.client.Get(url)
	if err != nil {
		log.Panicf("[TestCase.createPipe] error get %s. err: %s", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		log.Infof("[TestCase.createPipe] %s already exists")
		return
	}

	if resp.StatusCode != http.StatusNotFound {
		body, _ := ioutil.ReadAll(resp.Body)
		log.Panicf("[TestCase.createPipe] error get %s. status: %d, body: %s", url, resp.StatusCode, body)
	}

	cfg := tc.pipelineConfig()
	b, err := json.Marshal(cfg)
	if err != nil {
		log.Panic(err)
	}
	rawCfg := json.RawMessage(b)

	p := apiserver.ApiPipeline{
		ObjectMeta: v1.ObjectMeta{
			Name: tc.fullName,
		},
		Spec: apiserver.ApiPipelineSpec{
			Config: &rawCfg,
		},
	}

	req, err := json.Marshal(p)
	if err != nil {
		log.Panic(err)
	}
	resp, err = tc.client.Post(tc.operatorAddr+"/pipeline", "application/json", bytes.NewReader(req))
	if err != nil {
		log.Panic(err)
	}
	if resp.StatusCode != http.StatusCreated {
		body, _ := ioutil.ReadAll(resp.Body)
		log.Panicf("[TestCase.createPipe] error create pipeline. status: %d, body: %s", resp.StatusCode, string(body))
	}
}

func (tc *TestCase) pipelineConfig() config.PipelineConfigV2 {
	sourceDBConfig := getConfigFromDB(tc.sourceDB)
	targetDBConfig := getConfigFromDB(tc.targetDB)
	// create pipeline
	tableConfigs := []map[string]interface{}{
		{
			"schema": tc.fullName,
			"table":  "*",
		},
	}
	return config.PipelineConfigV2{
		PipelineName: tc.fullName,
		InputPlugins: map[string]interface{}{
			"mysql": map[string]interface{}{
				"source": map[string]interface{}{
					"host":     sourceDBConfig.Host,
					"username": sourceDBConfig.Username,
					"password": sourceDBConfig.Password,
					"port":     sourceDBConfig.Port,
					"location": sourceDBConfig.Location,
				},
				"table-configs": tableConfigs,
				"mode":          "replication",
			},
		},
		OutputPlugins: map[string]interface{}{
			"mysql": map[string]interface{}{
				"target": map[string]interface{}{
					"host":     targetDBConfig.Host,
					"username": targetDBConfig.Username,
					"password": targetDBConfig.Password,
					"port":     targetDBConfig.Port,
					"location": targetDBConfig.Location,
				},
				"routes": []map[string]interface{}{
					{
						"match-schema": tc.fullName,
					},
				},
			},
		},
	}
}

func (tc *TestCase) deletePipe() {
	url := fmt.Sprintf("%s/pipeline/%s", tc.operatorAddr, tc.fullName)
	request, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		log.Panicf("[TestCase.deletePipe] error NewRequest: %s", err)
	}
	_, err = tc.client.Do(request)
	if err != nil {
		log.Panicf("[TestCase.deletePipe] error: %s", err)
	}
}

func (tc *TestCase) prepareDB() {
	ddl := fmt.Sprintf("create database if not exists `%s`", tc.fullName)
	_, err := tc.sourceDB.Exec(ddl)
	if err != nil {
		log.Panicf("[TestCase.prepareDB] err %s", err)
	}

	_, err = tc.targetDB.Exec(ddl)
	if err != nil {
		log.Panicf("[TestCase.prepareDB] err %s", err)
	}
}

func (tc *TestCase) dropDB() {
	ddl := fmt.Sprintf("drop database if exists `%s`", tc.fullName)
	_, err := tc.sourceDB.Exec(ddl)
	if err != nil {
		log.Panicf("[TestCase.dropDB] source err %s", err)
	}

	_, err = tc.targetDB.Exec(ddl)
	if err != nil {
		log.Panicf("[TestCase.dropDB] target err %s", err)
	}
}

func getConfigFromDB(db *sql.DB) *utils.DBConfig {
	v := reflect.ValueOf(db).Elem()
	connector := v.FieldByName("connector").Elem()
	dsn := connector.FieldByName("dsn").String()
	driver := connector.FieldByName("driver").Elem().Type()
	if driver == reflect.ValueOf((*mysql.MySQLDriver)(nil)).Type() {
		cfg, err := mysql.ParseDSN(dsn)
		if err != nil {
			log.Panic(err)
		}
		addr := strings.Split(cfg.Addr, ":")
		port, err := strconv.Atoi(addr[1])
		if err != nil {
			log.Panic(err)
		}
		config := &utils.DBConfig{
			Host:                   addr[0],
			Location:               cfg.Loc.String(),
			Username:               cfg.User,
			Password:               cfg.Passwd,
			Port:                   port,
			Schema:                 cfg.DBName,
			MaxIdle:                int(v.FieldByName("maxIdle").Int()),
			MaxOpen:                int(v.FieldByName("maxOpen").Int()),
			MaxLifeTimeDurationStr: time.Duration(v.FieldByName("maxLifetime").Int()).String(),
		}
		err = config.ValidateAndSetDefault()
		if err != nil {
			log.Panic(err)
		}
		return config
	}

	log.Panicf("unknown driver type %s", driver)
	return nil
}
