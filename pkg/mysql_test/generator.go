package mysql_test

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"

	"github.com/moiot/gravity/pkg/utils/retry"
)

type GeneratorConfig struct {
	NrTables          int     `json:"nrTables" yaml:"nrTables"`
	NrSeedRows        int     `json:"nrSeedRows" yaml:"nrSeedRows"`
	DeleteRatio       float32 `json:"deleteRatio" yaml:"deleteRatio"`
	InsertRatio       float32 `json:"insertRatio" yaml:"insertRatio"`
	Concurrency       int     `json:"concurrency" yaml:"concurrency"`
	TransactionLength int     `json:"transactionLength" yaml:"transactionLength"`
}
type Generator struct {
	GeneratorConfig

	SourceDB     *sql.DB
	SourceSchema string

	TargetDB     *sql.DB
	TargetSchema string

	tableNames          []string
	tableDataGenerators []MysqlTableDataGenerator
	rands               []*rand.Rand
}

const tableDef = `
CREATE TABLE IF NOT EXISTS ` + "`%s`.`%s`" + `(
id BIGINT unsigned NOT NULL,
i INT DEFAULT 0,
ii INT,
ui INT unsigned,
ti  tinyint(4) DEFAULT NULL,
de decimal(11, 3),
fl float(11,3) NOT NULL,
do double(25, 3),
# see https://github.com/moiot/gravity/issues/59
#dt DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
#ts TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
dt DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
tbl TINYBLOB,
tte TINYTEXT CHARACTER SET utf8mb4,
ch  CHAR(5) CHARACTER SET utf8mb4,
va varchar(31) CHARACTER SET utf8mb4,
lva varchar(5000) CHARACTER SET utf8mb4,
j json DEFAULT NULL,
PRIMARY KEY (id),
UNIQUE KEY (i, ui),
UNIQUE KEY (ii)
)ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
`

const tablePrefix = "test_"

func (g *Generator) SetupTestTables(createTarget bool) []string {
	g.rands = make([]*rand.Rand, g.Concurrency)
	for i := 0; i < g.Concurrency; i++ {
		g.rands[i] = rand.New(rand.NewSource(time.Now().UnixNano()))
	}

	g.tableNames = make([]string, g.NrTables, g.NrTables)
	g.tableDataGenerators = make([]MysqlTableDataGenerator, g.NrTables, g.NrTables)
	for i := 0; i < g.NrTables; i++ {
		tableName := tablePrefix + strconv.Itoa(i)
		g.tableNames[i] = tableName

		s1 := fmt.Sprintf(tableDef, g.SourceSchema, tableName)
		if _, err := g.SourceDB.Exec(s1); err != nil {
			panic(fmt.Sprintf("failed to create table %v, err: %v", tableName, err.Error()))
		}

		if createTarget {
			s2 := fmt.Sprintf(tableDef, g.TargetSchema, tableName)
			if _, err := g.TargetDB.Exec(s2); err != nil {
				panic(fmt.Sprintf("failed to create table %v, err: %v", tableName, err.Error()))
			}
		}

		g.tableDataGenerators[i] = NewMysqlTableDataGenerator(g.SourceDB, g.SourceSchema, tableName)
	}
	return g.tableNames
}

type seedTask struct {
	g   MysqlTableDataGenerator
	num int
}

const seedBatch = 500

func (g *Generator) SeedRows() {
	if g.NrSeedRows == 0 {
		return
	}
	wg := &sync.WaitGroup{}
	wg.Add(g.Concurrency)
	taskC := make(chan seedTask, (g.NrSeedRows/seedBatch+1)*len(g.tableNames))

	for i := 0; i < g.Concurrency; i++ {
		go func(concurrentIdx int) {
			defer wg.Done()
			for t := range taskC {
				stmt, args := t.g.InitData(t.num, g.rands[concurrentIdx])
				_, err := g.SourceDB.Exec(stmt, args...)
				if err != nil {
					panic(fmt.Sprintf("failed to replace table: %v, err: %v", t.g.table, err.Error()))
				}
			}
		}(i)
	}

	for i := range g.tableNames {
		dataGenerator := g.tableDataGenerators[i]
		for i := 0; i < g.NrSeedRows/seedBatch; i++ {
			taskC <- seedTask{dataGenerator, seedBatch}
		}
		if g.NrSeedRows%seedBatch > 0 {
			taskC <- seedTask{dataGenerator, g.NrSeedRows % seedBatch}
		}
	}
	close(taskC)
	wg.Wait()
}

func (g *Generator) ParallelUpdate(ctx context.Context) *sync.WaitGroup {
	wg := &sync.WaitGroup{}
	wg.Add(g.Concurrency)
	for i := 0; i < g.Concurrency; i++ {
		go func(idx int) {
			defer wg.Done()
			g.execArbitraryTxn(ctx, idx, g.rands[idx])
		}(i)
	}
	return wg
}

func (g *Generator) TestChecksum() error {
	var c1, c2, tableName string
	for _, tableName = range g.tableNames {
		for i := 0; i < 5; i++ {
			c1 = TableChecksum(g.SourceDB, g.SourceSchema, tableName)
			c2 = TableChecksum(g.TargetDB, g.TargetSchema, tableName)
			if c1 != c2 {
				time.Sleep(time.Second)
			} else {
				break
			}
		}

		if c1 != c2 {
			return fmt.Errorf("checksum not equal sourceSchemaName: %v, targetSchemaName: %v, tableName: %v, source checksum: %v, target checksum: %v",
				g.SourceSchema, g.TargetSchema, tableName, c1, c2)
		}
	}

	return nil
}

func (g *Generator) execArbitraryTxn(ctx context.Context, idx int, r *rand.Rand) {
	var ddls = []string{
		fmt.Sprintf("CREATE TABLE `%s`.`_aa_%d` (  `ID` int(11) NOT NULL AUTO_INCREMENT,  `Name` char(35) NOT NULL DEFAULT '', PRIMARY KEY (`ID`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;", g.SourceSchema, idx),
		fmt.Sprintf("alter table `%s`.`_aa_%d` add column `foo` int default 0", g.SourceSchema, idx),
		fmt.Sprintf("create user foo_%d", idx),
	}

	var revert = []string{
		fmt.Sprintf("drop table if exists `%s`.`_aa_%d`", g.SourceSchema, idx),
		fmt.Sprintf("drop table if exists `%s`.`_aa_%d`", g.SourceSchema, idx),
		fmt.Sprintf("drop user foo_%d", idx),
	}

	executedDDL := make(map[int]bool)

	for {
		select {
		case <-ctx.Done():
			for k := range executedDDL {
				_, err := g.SourceDB.Exec(revert[k])
				if err != nil {
					log.Errorf("error exec: %s. err: %s", revert[k], err)
				}
			}
			return

		default:
			if len(executedDDL) < len(ddls) && r.Float32() < 0.05 {
				for i := range ddls {
					if !executedDDL[i] {
						_, err := g.SourceDB.Exec(ddls[i])
						if err != nil {
							log.Error("error exec ", ddls[i], ". err: ", err)
						} else {
							executedDDL[i] = true
						}
					}
				}
				continue
			}

			transactionLength := 10
			if g.TransactionLength > 0 {
				transactionLength = g.TransactionLength
			}
			num := r.Intn(transactionLength) + 1
			err := retry.Do(func() error {
				tx, err := g.SourceDB.Begin()
				if err != nil {
					return err
				}
				selectedTables := make([]int, num, num)
				for i := 0; i < num; i++ {
					selectedTables[i] = r.Intn(len(g.tableNames))
				}
				sort.Ints(selectedTables) //sort to reduce deadlock

				for _, tblIdx := range selectedTables {
					stmt, args := g.tableDataGenerators[tblIdx].RandomStmt(g.DeleteRatio, g.InsertRatio, r)
					_, err = tx.Exec(stmt, args...)
					if err != nil {
						tx.Rollback()
						return err
					}
				}

				err = tx.Commit()
				if err != nil {
					tx.Rollback()
					return err
				}
				return nil
			}, 3, retry.DefaultSleep)
			if err != nil {
				panic(err)
			}
		}
	}
}

func TableChecksum(db *sql.DB, dbName string, tableName string) string {
	var t string
	var checksum string

	row := db.QueryRow(fmt.Sprintf("CHECKSUM TABLE `%s`.`%s`", dbName, tableName))

	if err := row.Scan(&t, &checksum); err != nil {
		panic(fmt.Sprintf("err: %v", err.Error()))
	}

	return checksum
}

func TestChecksum(t *testing.T, tableNames []string, sourceDB *sql.DB, sourceDBName string, targetDB *sql.DB, targetDBName string) {
	assertions := assert.New(t)

	for _, tableName := range tableNames {
		c1 := TableChecksum(sourceDB, sourceDBName, tableName)
		c2 := TableChecksum(targetDB, targetDBName, tableName)
		if c1 != c2 {
			assertions.FailNowf("checksum not equal", "sourceDBName: %v, targetDBName: %v, tableName: %v, source checksum: %v, target checksum: %v",
				sourceDBName, targetDBName, tableName, c1, c2)
		}
	}
}
