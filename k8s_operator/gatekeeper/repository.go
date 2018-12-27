package gatekeeper

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"sort"
	"strings"

	"github.com/juju/errors"
	"gopkg.in/yaml.v2"
	"k8s.io/apimachinery/pkg/apis/meta/v1"

	clusterapi "github.com/moiot/gravity/k8s_operator/pkg/apis/cluster/v1alpha1"
	clusterclient "github.com/moiot/gravity/k8s_operator/pkg/client/cluster/clientset/versioned/typed/cluster/v1alpha1"
)

const dbName = "gatekeeper"
const tableName = "version"
const defaultRuleGroup = "default"

var fullTableName = fmt.Sprintf("`%s`.`%s`", dbName, tableName)

var createTable = fmt.Sprintf("create table if not exists %s(`name` varchar(50) NOT NULL, `cases` varchar(255) NOT NULL DEFAULT '', `created_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (`name`))ENGINE=InnoDB DEFAULT CHARSET=utf8;",
	fullTableName)

type Repository struct {
	operatorAddr  string
	clusterClient clusterclient.DrcClusterInterface

	sourceDB *sql.DB
	targetDB *sql.DB

	cases    []TestCaseConfig
	versions map[string]*Version
}

func NewRepository(casePath string, operatorAddr string, sourceDB *sql.DB, targetDB *sql.DB, clusterClient clusterclient.DrcClusterInterface) *Repository {
	_, err := sourceDB.Exec(fmt.Sprintf("create database if not exists `%s`", dbName))
	if err != nil {
		log.Panicf("error init db: %s", err)
	}

	_, err = sourceDB.Exec(createTable)
	if err != nil {
		log.Panicf("error init table: %s", err)
	}

	bytes, err := ioutil.ReadFile(casePath)
	if err != nil {
		log.Panic(err)
	}

	r := Repository{
		operatorAddr:  operatorAddr,
		clusterClient: clusterClient,
		sourceDB:      sourceDB,
		targetDB:      targetDB,
		versions:      make(map[string]*Version),
	}

	err = yaml.UnmarshalStrict(bytes, &r.cases)
	if err != nil {
		log.Panic(err)
	}

	rows, err := sourceDB.Query(fmt.Sprintf("select name, cases from %s", fullTableName))
	if err != nil {
		log.Panic(err)
	}
	defer rows.Close()

	for rows.Next() {
		v := &Version{}
		var cases string
		if err = rows.Scan(&v.Name, &cases); err != nil {
			log.Panic(err)
		}
		err = json.Unmarshal([]byte(cases), &v.RunningCases)
		if err != nil {
			log.Panic(err)
		}
		v.Reconcile(r.cases, r.operatorAddr, r.sourceDB, r.targetDB, false)
		r.versions[v.Name] = v
	}
	if err = rows.Err(); err != nil {
		log.Panic(err)
	}

	return &r
}

func (r *Repository) insert(v *Version) error {
	c := r.getCluster()
	defaultRule := getDefaultRule(c.Spec.DeploymentRules)

	rule := clusterapi.DeploymentRule{
		Group:     v.Name,
		Pipelines: []string{v.Name + "*"},
		Image:     removeImageTag(defaultRule.Image) + v.Name,
		Command:   defaultRule.Command,
	}
	c.Spec.DeploymentRules = append([]clusterapi.DeploymentRule{rule}, c.Spec.DeploymentRules...)
	c, err := r.clusterClient.Update(c)
	if err != nil {
		return errors.Annotatef(err, "error add deployment rule %#v.", rule)
	}

	v.Reconcile(r.cases, r.operatorAddr, r.sourceDB, r.targetDB, true)
	cases, _ := json.Marshal(v.RunningCases)
	_, err = r.sourceDB.Exec(fmt.Sprintf("insert into %s(name, cases) values (?, ?)", fullTableName), v.Name, string(cases))
	if err != nil {
		v.Delete()
		return errors.Trace(err)
	}

	r.versions[v.Name] = v
	return nil
}

func removeImageTag(s string) string {
	p := strings.LastIndex(s, ":")
	return s[:p+1]
}

func getDefaultRule(rules []clusterapi.DeploymentRule) clusterapi.DeploymentRule {
	for _, r := range rules {
		if r.Group == defaultRuleGroup {
			return r
		}
	}
	log.Fatalf("can't find default deployment rule")
	panic("impossible")
}

func (r *Repository) getCluster() *clusterapi.DrcCluster {
	ret, err := r.clusterClient.List(v1.ListOptions{})
	if err != nil {
		log.Fatalf("[Repository.insert] error list cluster: %s", err)
	}

	if len(ret.Items) != 1 {
		log.Fatalf("[Repository.insert] expect exactly one cluster object per namespace, actually %d", len(ret.Items))
	}

	return &ret.Items[0]
}

func (r *Repository) delete(name string) error {
	_, err := r.sourceDB.Exec(fmt.Sprintf("delete from %s where name = ?", fullTableName), name)
	if err != nil {
		return errors.Trace(err)
	}

	r.versions[name].Delete()
	delete(r.versions, name)

	c := r.getCluster()
	var rules []clusterapi.DeploymentRule
	for _, r := range c.Spec.DeploymentRules {
		if r.Group != name {
			rules = append(rules, r)
		}
	}
	c.Spec.DeploymentRules = rules
	c, err = r.clusterClient.Update(c)
	if err != nil {
		return errors.Annotatef(err, "error remove deployment rule %s.", name)
	}
	return nil
}

func (r *Repository) get(name string) *Version {
	return r.versions[name]
}

func (r *Repository) list() []*Version {
	ret := make([]*Version, 0, len(r.versions))
	for _, v := range r.versions {
		ret = append(ret, v)
	}
	sort.Slice(ret, func(i, j int) bool {
		return ret[i].Name < ret[j].Name
	})
	return ret
}
