package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"strconv"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"sort"

	"reflect"

	"github.com/BurntSushi/toml"

	"strings"

	"os"

	"os/signal"
	"syscall"

	"sync"

	"github.com/moiot/gravity/pkg/schema_store"
	"github.com/moiot/gravity/pkg/utils"
	"github.com/moiot/gravity/pkg/utils/retry"
)

type Config struct {
	Source      *utils.DBConfig `toml:"source"`
	Target      *utils.DBConfig `toml:"target"`
	SourceTable string          `toml:"source-table"`
	TargetTable string          `toml:"target-table"`
	Shading     bool            `toml:"shading"`
	ColName     string          `toml:"col-name"`
	ColType     string          `toml:"col-type"`
	MinString   string          `toml:"min-string"`
	MaxString   string          `toml:"max-string"`
	ValPrefix   string          `toml:"prefix"`
	Parallel    int             `toml:"parallel"`
}

func main() {
	var (
		config = flag.String("config", "verifier.conf", "path to config file")
	)

	flag.Parse()

	c := &Config{}
	_, err := toml.DecodeFile(*config, c)
	if err != nil {
		log.Fatalf("error parse config. %s", err)
	}

	source, err := utils.CreateDBConnection(c.Source)
	if err != nil {
		log.Fatalf("error connect to %#v. err: %s", c.Source, err)
	}
	source.SetMaxOpenConns(100)
	defer source.Close()

	target, err := utils.CreateDBConnection(c.Target)
	if err != nil {
		log.Fatalf("error connect to %#v. err: %s", c.Target, err)
	}
	target.SetMaxOpenConns(100)
	defer target.Close()

	var prefix = c.ValPrefix

	ret := make(chan TaskResult)
	tasks := make(chan Task, 10000)
	inFlight := &sync.WaitGroup{}
	initialMin := stringToInt64(c.MinString, c.ColType)
	initialMax := stringToInt64(c.MaxString, c.ColType)

	fmt.Printf("InitialMin: %d, InitialMax: %d, distance: %d\n", initialMin, initialMax, initialMax-initialMin)

	if !c.Shading {
		inFlight.Add(1)
		tasks <- Task{
			initialMin,
			c.MinString,
			initialMax,
			c.MaxString,
			c.SourceTable,
			c.TargetTable,
		}
	} else {
		sourceTables, err := schema_store.GetTablesFromDB(source, c.Source.Schema)
		if err != nil {
			log.Fatalf("error GetTablesFromDB. err: %s", err)
		}
		sourceTables = filter(sourceTables, c.SourceTable)

		targetTables, err := schema_store.GetTablesFromDB(target, c.Target.Schema)
		if err != nil {
			log.Fatalf("error GetTablesFromDB. err: %s", err)
		}
		targetTables = filter(targetTables, c.TargetTable)

		if len(sourceTables) != len(targetTables) {
			log.Fatalf("souce has %d tables, target has %d.", len(sourceTables), len(targetTables))
		}

		sort.Strings(sourceTables)
		sort.Strings(targetTables)

		if !reflect.DeepEqual(sourceTables, targetTables) {
			log.Fatalf("source and target don't have identical tables.")
		}

		inFlight.Add(len(sourceTables))
		for _, t := range sourceTables {
			tasks <- Task{
				initialMin,
				c.MinString,
				initialMax,
				c.MaxString,
				t,
				t,
			}
		}
	}

	go func() {
		inFlight.Wait()
		close(tasks)
		close(ret)
	}()

	for i := 0; i < c.Parallel; i++ {
		w := Worker{
			tasks,
			ret,
			source,
			target,
			prefix,
			c.ColName,
			c.ColType,
		}

		go w.run(inFlight)
	}

	resultView := make(map[string]*TableResult)
	sc := make(chan os.Signal, 1)
	signal.Notify(sc, syscall.SIGKILL, syscall.SIGQUIT)

	var cnt int
out:
	for {
		select {
		case taskResult, ok := <-ret:
			if ok {
				cnt++
				key := fmt.Sprintf("%s-%s", taskResult.sourceTable, taskResult.targetTable)
				tr := resultView[key]
				if tr == nil {
					tr = &TableResult{sourceTable: taskResult.sourceTable, targetTable: taskResult.targetTable}
					resultView[key] = tr
				}

				tr.taskResult = append(tr.taskResult, taskResult)

				if cnt%10 == 0 {
					log.Println("task queue length: ", len(tasks))
					log.Println(fmt.Sprintf("%s: %s ~ %s: %d - %d = %d", taskResult.sourceTable, taskResult.minString, taskResult.maxString, taskResult.targetCnt, taskResult.sourceCnt, taskResult.diff))
				}
			} else {
				log.Println("completed")
				break out
			}
		case <-sc:
			log.Println("interrupted")
			break out
		}
	}

	log.Println("results: ")

	keys := make([]string, 0, len(resultView))
	for k := range resultView {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})

	for _, k := range keys {
		resultView[k].complete()
		log.Println(resultView[k])
		log.Println()
		log.Println()
	}

	log.Println("done")
}

func filter(allTableNames []string, pattern string) []string {
	var ret []string
	for _, t := range allTableNames {
		if utils.Glob(pattern, t) {
			ret = append(ret, t)
		}
	}
	return ret
}

type Task struct {
	min         int64
	minString   string
	max         int64
	maxString   string
	sourceTable string
	targetTable string
}

type TaskResult struct {
	Task
	sourceCnt        int
	targetCnt        int
	diff             int
	comparisionCount int
}

type TableResult struct {
	sourceTable string
	targetTable string
	taskResult  []TaskResult
}

func (tr *TableResult) String() string {
	var tbl string
	if tr.sourceTable == tr.targetTable {
		tbl = tr.sourceTable
	} else {
		tbl = fmt.Sprintf("%s-%s", tr.sourceTable, tr.targetTable)
	}

	sb := strings.Builder{}
	sb.WriteString(tbl)
	sb.WriteString(":\n")
	for _, m := range tr.taskResult {
		sb.WriteString("\t")
		sb.WriteString(fmt.Sprintf("%s ~ %s: %d - %d = %d", m.minString, m.maxString, m.targetCnt, m.sourceCnt, m.diff))
		sb.WriteString("\n")
	}
	return sb.String()
}

func (tr *TableResult) complete() {
	var filtered []TaskResult
	for i := 0; i < len(tr.taskResult)-1; i++ {
		m := tr.taskResult[i]
		if m.diff == 0 {
			continue
		}
		included := false
		for j := i + 1; j < len(tr.taskResult); j++ {
			n := tr.taskResult[j]
			if n.min >= m.min && n.max <= m.max && n.diff == m.diff {
				included = true
				break
			}
		}

		if !included {
			filtered = append(filtered, m)
		}
	}
	tr.taskResult = filtered

	sort.Slice(tr.taskResult, func(i, j int) bool {
		if tr.taskResult[i].minString == tr.taskResult[j].minString {
			return tr.taskResult[i].maxString < tr.taskResult[j].maxString
		}

		return tr.taskResult[i].minString < tr.taskResult[j].minString
	})
}

type Worker struct {
	taskQ   chan Task
	resultQ chan TaskResult
	source  *sql.DB
	target  *sql.DB
	prefix  string
	col     string
	colType string
}

func (w *Worker) run(inFlight *sync.WaitGroup) {
	comparisionCount := 0
	for task := range w.taskQ {

		sourceSQL, sourceCurrentMin, sourceCurrentMax := stmt(w.prefix, task.min, task.max, task.sourceTable, w.col, w.colType)
		sourceCnt := count(w.source, sourceSQL, sourceCurrentMin, sourceCurrentMax)

		targetSQL, targetCurrentMin, targetCurrentMax := stmt(w.prefix, task.min, task.max, task.targetTable, w.col, w.colType)
		targetCnt := count(w.target, targetSQL, targetCurrentMin, targetCurrentMax)

		comparisionCount++
		result := TaskResult{task, sourceCnt, targetCnt, targetCnt - sourceCnt, comparisionCount}
		w.resultQ <- result

		if sourceCnt != targetCnt && task.min != task.max {

			// If range and the result diff is small enough, we break.
			if (task.max-task.min < 10 || (sourceCnt < 20 && targetCnt < 20)) && result.diff < 10 {
				inFlight.Done()
				continue
			}

			middle := (task.min + task.max) / 2

			inFlight.Add(2) //add before done to prevent false finish
			inFlight.Done()

			w.taskQ <- Task{
				task.min,
				int64ToString(task.min, w.colType, w.prefix),
				middle,
				int64ToString(middle, w.colType, w.prefix),
				task.sourceTable,
				task.targetTable,
			}
			w.taskQ <- Task{
				middle + 1,
				int64ToString(middle+1, w.colType, w.prefix),
				task.max,
				int64ToString(task.max, w.colType, w.prefix),
				task.sourceTable,
				task.targetTable,
			}
		} else {
			inFlight.Done()
		}
	}
}

func count(db *sql.DB, stmt string, currentMin interface{}, currentMax interface{}) int {
	var cnt int
	err := retry.Do(func() error {
		return db.QueryRow(stmt, currentMin, currentMax).Scan(&cnt)
	}, 3, 0)
	if err != nil {
		log.Fatal("error count: ", stmt, ", err: ", err)
	}
	return cnt
}

func stmt(valPrefix string, min int64, max int64, tableName, colName string, colType string) (string, interface{}, interface{}) {
	return fmt.Sprintf("select count(*) from %s where %s between ? and ?", tableName, colName), int64ToVal(min, colType, valPrefix), int64ToVal(max, colType, valPrefix)
}

func int64ToVal(v int64, colType string, prefix string) interface{} {
	// if prefix exists, then it must be a string
	if prefix != "" {
		return fmt.Sprintf("%s%d", prefix, v)
	}

	switch colType {
	case "time":
		return time.Unix(v, 0)
	case "string":
		return fmt.Sprintf("%d", v)
	case "int":
		return v
	default:
		panic("not supported right now")
	}
}

func int64ToString(v int64, colType string, prefix string) string {
	if prefix != "" {
		return fmt.Sprintf("%s%d", prefix, v)
	}

	switch colType {
	case "time":
		return fmt.Sprintf("%s", time.Unix(v, 0))
	case "int", "string":
		return fmt.Sprintf("%d", v)
	default:
		panic("not supported right now")
	}
}

func stringToInt64(v string, colType string) int64 {
	if colType == "int" {
		i, err := strconv.Atoi(v)
		if err != nil {
			log.Fatalf("failed to parse %s to int", v)
		}
		return int64(i)
	}

	if colType == "time" {
		t, err := time.Parse("2006-01-02T15:04:05Z07:00", v)
		if err != nil {
			log.Fatalf("failed to parse %s to time. err: %s", v, err)
		}
		// return the number of minutes to reduce the search space
		return t.Unix()
	}
	panic("col type not supported")
}
