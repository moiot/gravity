package padder

//import (
//	"database/sql"
//	"fmt"
//	"os"
//	"path/filepath"
//	"time"
//
//	. "github.com/onsi/ginkgo"
//	. "github.com/onsi/gomega"
//	"github.com/siddontang/go-mysql/mysql"
//	"github.com/siddontang/go-mysql/replication"
//
//	"github.com/moiot/gravity/padder/config"
//	"github.com/moiot/gravity/pkg/mysql_test"
//	"github.com/moiot/gravity/pkg/utils"
//)
//
//const srcConfStr = `
//[padder.mysql.target]
//host = "source-db"
//username = "root"
//password = ""
//port = 3306`
//
//const targetConfStr = `
//[padder]
//enable-delete = true
//binlog-list = ["bin.001", "bin.002"]
//
//[padder.mysql.target]
//host = "target-db"
//username = "root"
//password = ""
//port = 3306
//schema = "test"
//
//[padder.mysql.start-position]
//binlog-name= "bin.001"
//binlog-pos= 1234`
//
//func buildBinLogList(mysqlConfig config.MySQLConfig, mysqlPosition utils.MySQLBinlogPosition, query func()) ([]string, error) {
//	syncerConfig := replication.BinlogSyncerConfig{
//		ServerID:  utils.GenerateRandomServerID(),
//		Flavor:    "mysql",
//		Host:      mysqlConfig.Target.Host,
//		Port:      uint16(mysqlConfig.Target.Port),
//		User:      mysqlConfig.Target.Username,
//		Password:  mysqlConfig.Target.Password,
//		ParseTime: true,
//	}
//
//	syncer := replication.NewBinlogSyncer(syncerConfig)
//	query()
//	err := syncer.StartBackup("./var", mysql.Position{Name: mysqlPosition.BinLogFileName, Pos: 0}, 50*time.Millisecond)
//	Expect(err).To(BeNil())
//	return func(path string) []string {
//		var binLogList []string
//		findStart := false
//		err := filepath.Walk(path, func(subPath string, f os.FileInfo, err error) error {
//			if f == nil {
//				return err
//			}
//			if f.IsDir() {
//				return nil
//			}
//			if f.Name() == mysqlPosition.BinLogFileName {
//				findStart = true
//			}
//			if findStart {
//				binLogList = append(binLogList, "./var/"+f.Name())
//			}
//			return nil
//		})
//		if err != nil {
//			fmt.Printf("filepath.Walk() returned %v\n", err)
//		}
//		return binLogList
//	}("./var"), nil
//
//}
//
//func testPreview(padderConfig config.PadderConfig, verify func(stats PreviewStatistic)) {
//	stats, err := Preview(padderConfig)
//	Expect(err).To(BeNil())
//	verify(stats)
//}
//
//func testPad(padderConfig config.PadderConfig, verify func()) {
//	err := Pad(padderConfig)
//	Expect(err).To(BeNil())
//	verify()
//}
//
//func getPadderConfig(sourceDB *sql.DB, srcConf config.MySQLConfig, targetConf config.MySQLConfig, enableDelete bool, query func()) config.PadderConfig {
//	db := utils.NewMySQLDB(sourceDB)
//	srcPosition, srcGtidSet, err := db.GetMasterStatus()
//	srcMysqlPosition := utils.MySQLBinlogPosition{
//		BinLogFileName: srcPosition.Name,
//		BinLogFilePos:  srcPosition.Pos,
//		BinlogGTID:     srcGtidSet.String(),
//	}
//	binLogs, err := buildBinLogList(srcConf, srcMysqlPosition, query)
//	Expect(err).To(BeNil())
//
//	targetConf.StartPosition = &srcMysqlPosition
//	return config.PadderConfig{
//		BinLogList:   binLogs,
//		MySQLConfig:  &targetConf,
//		EnableDelete: enableDelete,
//	}
//
//}
//
//var _ = Describe("padder test", func() {
//	Describe("Pad test", func() {
//		dbName := "testPadDBName"
//		sourceDB := mysql_test.MustSetupSourceDB(dbName)
//		targetDB := mysql_test.MustSetupTargetDB(dbName)
//		count := 4
//		var sourceMysqlConfig *config.MySQLConfig
//		var targetMysqlConfig *config.MySQLConfig
//		var idList []int
//		for id := 0; id < count; id++ {
//			idList = append(idList, id)
//		}
//		oldName := "oldName"
//		newName := "newName"
//		setupConfig := func() (*config.MySQLConfig, *config.MySQLConfig) {
//			srcConfig, err := config.CreateConfigFromString(srcConfStr)
//			Expect(err).To(BeNil())
//			targetConfig, err := config.CreateConfigFromString(targetConfStr)
//			Expect(err).To(BeNil())
//			srcMysqlConfig := srcConfig.PadderConfig.MySQLConfig
//			trgtMysqlConfig := targetConfig.PadderConfig.MySQLConfig
//			trgtMysqlConfig.Target.Schema = dbName
//			return srcMysqlConfig, trgtMysqlConfig
//		}
//		BeforeEach(func() {
//			_, err := sourceDB.Exec(fmt.Sprintf("truncate table %s.%s", dbName, mysql_test.TestTableName))
//			Expect(err).ShouldNot(HaveOccurred())
//			_, err = targetDB.Exec(fmt.Sprintf("truncate table %s.%s", dbName, mysql_test.TestTableName))
//			Expect(err).ShouldNot(HaveOccurred())
//			fmt.Println("before each executed")
//			sourceMysqlConfig, targetMysqlConfig = setupConfig()
//		})
//
//		Context("pad succeed test suite", func() {
//			It("pad insert succeed", func() {
//				insert := func() {
//					for id := range idList {
//						args := map[string]interface{}{
//							"id":   id,
//							"name": oldName,
//							"ts":   time.Now(),
//						}
//						err := mysql_test.InsertIntoTestTable(sourceDB, dbName, mysql_test.TestTableName, args)
//						Expect(err).To(BeNil())
//					}
//				}
//				verify := func() {
//					for id := range idList {
//						result, err := mysql_test.QueryTestTable(targetDB, dbName, mysql_test.TestTableName, id)
//						Expect(err).To(BeNil())
//						Expect(result).To(BeEquivalentTo(oldName))
//					}
//				}
//				testPad(getPadderConfig(sourceDB, *sourceMysqlConfig, *targetMysqlConfig, true, insert), verify)
//
//			})
//			It("pad update succeed", func() {
//				for id := range idList {
//					now := time.Now()
//					args := map[string]interface{}{
//						"id":   id,
//						"name": oldName,
//						"ts":   now,
//					}
//					err := mysql_test.InsertIntoTestTable(sourceDB, dbName, mysql_test.TestTableName, args)
//					Expect(err).To(BeNil())
//					err = mysql_test.InsertIntoTestTable(targetDB, dbName, mysql_test.TestTableName, args)
//					Expect(err).To(BeNil())
//				}
//				update := func() {
//					for id := range idList {
//						err := mysql_test.UpdateTestTable(sourceDB, dbName, mysql_test.TestTableName, id, newName)
//						Expect(err).To(BeNil())
//					}
//				}
//				verify := func() {
//					for id := range idList {
//						result, err := mysql_test.QueryTestTable(targetDB, dbName, mysql_test.TestTableName, id)
//						Expect(err).To(BeNil())
//						Expect(result).To(BeEquivalentTo(newName))
//					}
//				}
//				testPad(getPadderConfig(sourceDB, *sourceMysqlConfig, *targetMysqlConfig, true, update), verify)
//
//			})
//			It("pad delete succeed", func() {
//				for id := range idList {
//					args := map[string]interface{}{
//						"id":   id,
//						"name": oldName,
//						"ts":   time.Now(),
//					}
//					err := mysql_test.InsertIntoTestTable(sourceDB, dbName, mysql_test.TestTableName, args)
//					Expect(err).To(BeNil())
//				}
//				delete := func() {
//					for id := range idList {
//						err := mysql_test.DeleteTestTable(sourceDB, dbName, mysql_test.TestTableName, id)
//						Expect(err).To(BeNil())
//					}
//				}
//				verify := func() {
//					for id := range idList {
//						_, err := mysql_test.QueryTestTable(targetDB, dbName, mysql_test.TestTableName, id)
//						Expect(err).To(BeEquivalentTo(sql.ErrNoRows))
//					}
//				}
//				testPad(getPadderConfig(sourceDB, *sourceMysqlConfig, *targetMysqlConfig, true, delete), verify)
//			})
//
//		})
//
//		Context("skip test suite", func() {
//			sourceDBName := "sourceDBNameForSkipTest"
//			targetDBName := "dbNameForSkipTest"
//			sourceDB := mysql_test.MustSetupSourceDB(sourceDBName)
//			targetDB := mysql_test.MustSetupTargetDB(targetDBName)
//			count := 4
//			var sourceMysqlConfig *config.MySQLConfig
//			var targetMysqlConfig *config.MySQLConfig
//			var idList []int
//			for id := 0; id < count; id++ {
//				idList = append(idList, id)
//			}
//			oldName := "oldName"
//			It("skip none target db write event", func() {
//				sourceMysqlConfig, targetMysqlConfig = setupConfig()
//				targetMysqlConfig.Target.Schema = targetDBName
//				insert := func() {
//					for id := range idList {
//						args := map[string]interface{}{
//							"id":   id,
//							"name": oldName,
//							"ts":   time.Now(),
//						}
//						err := mysql_test.InsertIntoTestTable(sourceDB, sourceDBName, mysql_test.TestTableName, args)
//						Expect(err).To(BeNil())
//					}
//				}
//				verify := func() {
//					for id := range idList {
//						_, err := mysql_test.QueryTestTable(targetDB, targetDBName, mysql_test.TestTableName, id)
//						Expect(err).To(BeEquivalentTo(sql.ErrNoRows))
//					}
//				}
//				testPad(getPadderConfig(sourceDB, *sourceMysqlConfig, *targetMysqlConfig, true, insert), verify)
//				_, err := sourceDB.Exec(fmt.Sprintf("truncate table %s.%s", sourceDBName, mysql_test.TestTableName))
//				Expect(err).ShouldNot(HaveOccurred())
//				_, err = targetDB.Exec(fmt.Sprintf("truncate table %s.%s", targetDBName, mysql_test.TestTableName))
//				Expect(err).ShouldNot(HaveOccurred())
//			})
//
//		})
//
//		Context("disable delete test suite", func() {
//			It("skip delete if delete is disabled", func() {
//				for id := range idList {
//					now := time.Now()
//					args := map[string]interface{}{
//						"id":   id,
//						"name": oldName,
//						"ts":   now,
//					}
//					err := mysql_test.InsertIntoTestTable(sourceDB, dbName, mysql_test.TestTableName, args)
//					Expect(err).To(BeNil())
//					err = mysql_test.InsertIntoTestTable(targetDB, dbName, mysql_test.TestTableName, args)
//					Expect(err).To(BeNil())
//				}
//				delete := func() {
//					for id := range idList {
//						err := mysql_test.DeleteTestTable(sourceDB, dbName, mysql_test.TestTableName, id)
//						Expect(err).To(BeNil())
//					}
//				}
//				verify := func() {
//					for id := range idList {
//						result, err := mysql_test.QueryTestTable(targetDB, dbName, mysql_test.TestTableName, id)
//						Expect(err).To(BeNil())
//						Expect(result).To(BeEquivalentTo(oldName))
//					}
//				}
//				testPad(getPadderConfig(sourceDB, *sourceMysqlConfig, *targetMysqlConfig, false, delete), verify)
//			})
//
//		})
//		Context("pad with conflict test suite", func() {
//			It("pad insert with conflict", func() {
//				insert := func() {
//					now := time.Now()
//					for id := range idList {
//						args := map[string]interface{}{
//							"id":   id,
//							"name": oldName,
//							"ts":   now,
//						}
//						err := mysql_test.InsertIntoTestTable(sourceDB, dbName, mysql_test.TestTableName, args)
//						Expect(err).To(BeNil())
//					}
//					for id := range idList {
//						args := map[string]interface{}{
//							"id":   id,
//							"name": oldName,
//							"ts":   now,
//						}
//						err := mysql_test.InsertIntoTestTable(targetDB, dbName, mysql_test.TestTableName, args)
//						Expect(err).To(BeNil())
//					}
//				}
//				verify := func() {
//					for id := range idList {
//						result, err := mysql_test.QueryTestTable(targetDB, dbName, mysql_test.TestTableName, id)
//						Expect(err).To(BeNil())
//						Expect(result).To(BeEquivalentTo(oldName))
//					}
//				}
//				testPad(getPadderConfig(sourceDB, *sourceMysqlConfig, *targetMysqlConfig, true, insert), verify)
//
//			})
//
//			It("pad update with conflict", func() {
//				conflictName := "conflictName"
//				now := time.Now()
//				for id := range idList {
//					args := map[string]interface{}{
//						"id":   id,
//						"name": oldName,
//						"ts":   now,
//					}
//					err := mysql_test.InsertIntoTestTable(sourceDB, dbName, mysql_test.TestTableName, args)
//					Expect(err).To(BeNil())
//				}
//				for id := range idList {
//					args := map[string]interface{}{
//						"id":   id,
//						"name": oldName,
//						"ts":   now,
//					}
//					err := mysql_test.InsertIntoTestTable(targetDB, dbName, mysql_test.TestTableName, args)
//					Expect(err).To(BeNil())
//				}
//				update := func() {
//					for id := range idList {
//						err := mysql_test.UpdateTestTable(sourceDB, dbName, mysql_test.TestTableName, id, newName)
//						Expect(err).To(BeNil())
//					}
//					for id := range idList {
//						err := mysql_test.UpdateTestTable(targetDB, dbName, mysql_test.TestTableName, id, conflictName)
//						Expect(err).To(BeNil())
//					}
//				}
//				verify := func() {
//					for id := range idList {
//						result, err := mysql_test.QueryTestTable(targetDB, dbName, mysql_test.TestTableName, id)
//						Expect(err).To(BeNil())
//						Expect(result).To(BeEquivalentTo(conflictName))
//					}
//				}
//				testPad(getPadderConfig(sourceDB, *sourceMysqlConfig, *targetMysqlConfig, true, update), verify)
//			})
//
//			// If one row is deleted in target table before padding delete action from source table,
//			// we can only detect it in conflict log
//		})
//
//	})
//
//	Describe("Pad preview test", func() {
//		dbName := "testPreviewDBName"
//		sourceDB := mysql_test.MustSetupSourceDB(dbName)
//		targetDB := mysql_test.MustSetupTargetDB(dbName)
//		count := 4
//		var sourceMysqlConfig *config.MySQLConfig
//		var targetMysqlConfig *config.MySQLConfig
//		var idList []int
//		for id := 0; id < count; id++ {
//			idList = append(idList, id)
//		}
//		oldName := "oldName"
//		newName := "newName"
//		setupConfig := func() (*config.MySQLConfig, *config.MySQLConfig) {
//			srcConfig, err := config.CreateConfigFromString(srcConfStr)
//			Expect(err).To(BeNil())
//			targetConfig, err := config.CreateConfigFromString(targetConfStr)
//			Expect(err).To(BeNil())
//			srcMysqlConfig := srcConfig.PadderConfig.MySQLConfig
//			trgtMysqlConfig := targetConfig.PadderConfig.MySQLConfig
//			trgtMysqlConfig.Target.Schema = dbName
//			return srcMysqlConfig, trgtMysqlConfig
//		}
//		BeforeEach(func() {
//			_, err := sourceDB.Exec(fmt.Sprintf("truncate table %s.%s", dbName, mysql_test.TestTableWithoutTs))
//			Expect(err).ShouldNot(HaveOccurred())
//			_, err = targetDB.Exec(fmt.Sprintf("truncate table %s.%s", dbName, mysql_test.TestTableWithoutTs))
//			Expect(err).ShouldNot(HaveOccurred())
//			sourceMysqlConfig, targetMysqlConfig = setupConfig()
//		})
//		Context("pad preview with no conflict", func() {
//			verify := func(stats PreviewStatistic) {
//				Expect(stats.WriteEventCount).To(BeEquivalentTo(count))
//				Expect(stats.ConflictRatio).To(BeEquivalentTo(0))
//				Expect(stats.UnknownRatio).To(BeEquivalentTo(0))
//			}
//			It("preview insert with no conflict", func() {
//				insert := func() {
//					for id := range idList {
//						args := map[string]interface{}{
//							"id":   id,
//							"name": oldName,
//						}
//						err := mysql_test.InsertIntoTestTable(sourceDB, dbName, mysql_test.TestTableWithoutTs, args)
//						Expect(err).To(BeNil())
//					}
//				}
//				testPreview(getPadderConfig(sourceDB, *sourceMysqlConfig, *targetMysqlConfig, true, insert), verify)
//
//			})
//			It("preview update with no conflict", func() {
//				for id := range idList {
//					args := map[string]interface{}{
//						"id":   id,
//						"name": oldName,
//					}
//					err := mysql_test.InsertIntoTestTable(sourceDB, dbName, mysql_test.TestTableWithoutTs, args)
//					Expect(err).To(BeNil())
//					err = mysql_test.InsertIntoTestTable(targetDB, dbName, mysql_test.TestTableWithoutTs, args)
//					Expect(err).To(BeNil())
//				}
//				update := func() {
//					for id := range idList {
//						err := mysql_test.UpdateTestTable(sourceDB, dbName, mysql_test.TestTableWithoutTs, id, newName)
//						Expect(err).To(BeNil())
//					}
//				}
//				testPreview(getPadderConfig(sourceDB, *sourceMysqlConfig, *targetMysqlConfig, true, update), verify)
//
//			})
//
//			It("preview delete with no conflict", func() {
//				for id := range idList {
//					args := map[string]interface{}{
//						"id":   id,
//						"name": oldName,
//					}
//					err := mysql_test.InsertIntoTestTable(sourceDB, dbName, mysql_test.TestTableWithoutTs, args)
//					Expect(err).To(BeNil())
//					err = mysql_test.InsertIntoTestTable(targetDB, dbName, mysql_test.TestTableWithoutTs, args)
//					Expect(err).To(BeNil())
//				}
//				delete := func() {
//					for id := range idList {
//						err := mysql_test.DeleteTestTable(sourceDB, dbName, mysql_test.TestTableWithoutTs, id)
//						Expect(err).To(BeNil())
//					}
//				}
//				testPreview(getPadderConfig(sourceDB, *sourceMysqlConfig, *targetMysqlConfig, true, delete), verify)
//
//			})
//		})
//
//		Context("pad preview with conflict", func() {
//			verify := func(stats PreviewStatistic) {
//				Expect(stats.WriteEventCount).To(BeEquivalentTo(count))
//				Expect(stats.ConflictRatio).To(BeEquivalentTo(0.5))
//				Expect(stats.UnknownRatio).To(BeEquivalentTo(0))
//			}
//			It("preview insert with conflict", func() {
//				insert := func() {
//					for id := range idList {
//						args := map[string]interface{}{
//							"id":   id,
//							"name": oldName,
//						}
//						err := mysql_test.InsertIntoTestTable(sourceDB, dbName, mysql_test.TestTableWithoutTs, args)
//						Expect(err).To(BeNil())
//					}
//					for _, id := range idList[0 : count/2] {
//						args := map[string]interface{}{
//							"id":   id,
//							"name": oldName,
//						}
//						err := mysql_test.InsertIntoTestTable(targetDB, dbName, mysql_test.TestTableWithoutTs, args)
//						Expect(err).To(BeNil())
//					}
//				}
//				testPreview(getPadderConfig(sourceDB, *sourceMysqlConfig, *targetMysqlConfig, true, insert), verify)
//			})
//
//			It("preview update with conflict", func() {
//				for id := range idList {
//					args := map[string]interface{}{
//						"id":   id,
//						"name": oldName,
//					}
//					err := mysql_test.InsertIntoTestTable(sourceDB, dbName, mysql_test.TestTableWithoutTs, args)
//					Expect(err).To(BeNil())
//
//					err = mysql_test.InsertIntoTestTable(targetDB, dbName, mysql_test.TestTableWithoutTs, args)
//					Expect(err).To(BeNil())
//				}
//				update := func() {
//					for id := range idList {
//						err := mysql_test.UpdateTestTable(sourceDB, dbName, mysql_test.TestTableWithoutTs, id, newName)
//						Expect(err).To(BeNil())
//					}
//					for _, id := range idList[count/2 : count] {
//						err := mysql_test.UpdateTestTable(targetDB, dbName, mysql_test.TestTableWithoutTs, id, newName)
//						Expect(err).To(BeNil())
//					}
//				}
//				testPreview(getPadderConfig(sourceDB, *sourceMysqlConfig, *targetMysqlConfig, true, update), verify)
//			})
//			It("preview delete with conflict", func() {
//				for id := range idList {
//					args := map[string]interface{}{
//						"id":   id,
//						"name": oldName,
//					}
//					err := mysql_test.InsertIntoTestTable(sourceDB, dbName, mysql_test.TestTableWithoutTs, args)
//					Expect(err).To(BeNil())
//					err = mysql_test.InsertIntoTestTable(targetDB, dbName, mysql_test.TestTableWithoutTs, args)
//					Expect(err).To(BeNil())
//				}
//				delete := func() {
//					for id := range idList[count/2 : count] {
//						err := mysql_test.DeleteTestTable(targetDB, dbName, mysql_test.TestTableWithoutTs, id)
//						Expect(err).To(BeNil())
//					}
//					for id := range idList {
//						err := mysql_test.DeleteTestTable(sourceDB, dbName, mysql_test.TestTableWithoutTs, id)
//						Expect(err).To(BeNil())
//					}
//				}
//				testPreview(getPadderConfig(sourceDB, *sourceMysqlConfig, *targetMysqlConfig, true, delete), verify)
//			})
//
//		})
//		Context("pad preview skip delete", func() {
//			verify := func(stats PreviewStatistic) {
//				Expect(stats.WriteEventCount).To(BeEquivalentTo(count))
//				Expect(stats.ConflictRatio).To(BeEquivalentTo(0))
//				Expect(stats.UnknownRatio).To(BeEquivalentTo(1.0))
//			}
//			It("skip one delete will add one unknown", func() {
//				for id := range idList {
//					args := map[string]interface{}{
//						"id":   id,
//						"name": oldName,
//					}
//					err := mysql_test.InsertIntoTestTable(sourceDB, dbName, mysql_test.TestTableWithoutTs, args)
//					Expect(err).To(BeNil())
//					err = mysql_test.InsertIntoTestTable(targetDB, dbName, mysql_test.TestTableWithoutTs, args)
//					Expect(err).To(BeNil())
//				}
//				delete := func() {
//					for _, id := range idList {
//						err := mysql_test.DeleteTestTable(sourceDB, dbName, mysql_test.TestTableWithoutTs, id)
//						Expect(err).To(BeNil())
//					}
//				}
//				testPreview(getPadderConfig(sourceDB, *sourceMysqlConfig, *targetMysqlConfig, false, delete), verify)
//
//			})
//
//		})
//
//		Context("preview mix test", func() {
//			verify := func(stats PreviewStatistic) {
//				Expect(stats.WriteEventCount).To(BeEquivalentTo(count))
//				Expect(stats.ConflictRatio).To(BeEquivalentTo(0.25))
//				Expect(stats.UnknownRatio).To(BeEquivalentTo(0.25))
//			}
//			It("preview contains conflict and unknown", func() {
//				args := map[string]interface{}{
//					"id":   0,
//					"name": oldName,
//				}
//
//				err := mysql_test.InsertIntoTestTable(targetDB, dbName, mysql_test.TestTableWithoutTs, args)
//				Expect(err).To(BeNil())
//				// insert 3 rows with 1 conflict, then delete 1 rows with 1 unknown
//				query := func() {
//					for id := range idList[0 : count-1] {
//						args := map[string]interface{}{
//							"id":   id,
//							"name": oldName,
//						}
//						err := mysql_test.InsertIntoTestTable(sourceDB, dbName, mysql_test.TestTableWithoutTs, args)
//						Expect(err).To(BeNil())
//					}
//					err := mysql_test.DeleteTestTable(sourceDB, dbName, mysql_test.TestTableWithoutTs, 0)
//					Expect(err).To(BeNil())
//				}
//				testPreview(getPadderConfig(sourceDB, *sourceMysqlConfig, *targetMysqlConfig, false, query), verify)
//
//			})
//
//		})
//	})
//
//})
