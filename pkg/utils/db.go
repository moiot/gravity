package utils

import (
	"database/sql"
	"fmt"
	"math"
	"math/rand"
	"net/url"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/moiot/gravity/pkg/config"

	_ "github.com/go-sql-driver/mysql"
	"github.com/juju/errors"
	gomysql "github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	log "github.com/sirupsen/logrus"
)

func getIndexRowsName(db *sql.DB, statement string) ([]string, error) {
	resultRows, err := GetIndexRows(db, statement)
	if err != nil {
		return nil, errors.Trace(err)
	}

	var result []string
	for _, r := range resultRows {
		result = append(result, r[6].String)
	}
	return result, nil
}

func IsTableEmpty(db *sql.DB, schema string, table string, condition string) bool {
	where := ""
	if condition != "" {
		where = "WHERE " + condition
	}
	r := db.QueryRow(fmt.Sprintf("select count(*) from (select * from `%s`.`%s` %s limit 1)t", schema, table, where))
	var cnt int
	if err := r.Scan(&cnt); err != nil {
		log.Fatalf("error IsTableEmpty, schema %s, table %s, err %s", schema, table, errors.Trace(err))
	}
	return cnt == 0
}

func GetIndexRows(db *sql.DB, statement string) ([][]sql.NullString, error) {
	rows, err := db.Query(statement)
	defer func() {
		if rows != nil {
			rows.Close()
		}
	}()
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, errors.Trace(err)
	}

	/*
		*************************** 1. row ***************************
		           CONSTRAINT_CATALOG: def
		            CONSTRAINT_SCHEMA: _gravity
		              CONSTRAINT_NAME: PRIMARY
		                TABLE_CATALOG: def
		                 TABLE_SCHEMA: _gravity
		                   TABLE_NAME: _gravity_txn_tags
		                  COLUMN_NAME: id
		             ORDINAL_POSITION: 1
		POSITION_IN_UNIQUE_CONSTRAINT: NULL
		      REFERENCED_TABLE_SCHEMA: NULL
		        REFERENCED_TABLE_NAME: NULL
		       REFERENCED_COLUMN_NAME: NULL
	*/
	var resultRows [][]sql.NullString

	for rows.Next() {
		values := make([]sql.NullString, len(columns))
		scanArgs := make([]interface{}, len(columns))
		for i := range values {
			scanArgs[i] = &values[i]
		}

		if err := rows.Scan(scanArgs...); err != nil {
			return nil, errors.Trace(err)
		}

		resultRows = append(resultRows, values)
	}
	return resultRows, nil
}

func GetPrimaryKeys(db *sql.DB, schemaName string, tableName string) ([]string, error) {

	statement := fmt.Sprintf("SELECT * FROM information_schema.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s' AND CONSTRAINT_NAME = 'PRIMARY' ORDER BY ORDINAL_POSITION", schemaName, tableName)
	return getIndexRowsName(db, statement)
}

func GetUniqueIndexesWithoutPks(db *sql.DB, schemaName string, tableName string) ([]string, error) {
	statement := fmt.Sprintf("SELECT * FROM information_schema.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s' AND CONSTRAINT_NAME != 'PRIMARY' AND POSITION_IN_UNIQUE_CONSTRAINT IS NULL ORDER BY ORDINAL_POSITION", schemaName, tableName)
	return getIndexRowsName(db, statement)
}

func EstimateRowsCount(db *sql.DB, schemaName string, tableName string) (int64, error) {
	statement := fmt.Sprintf("SELECT TABLE_ROWS FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s'", schemaName, tableName)
	var rowsCount sql.NullInt64

	row := db.QueryRow(statement)
	if err := row.Scan(&rowsCount); err != nil {
		return 0, errors.Trace(err)
	}

	if !rowsCount.Valid {
		return 0, errors.Errorf("TABLE_ROWS null")
	}

	return rowsCount.Int64, nil
}

//
// We need to look at this file to check each type
// https://github.com/go-sql-driver/mysql/blob/master/fields.go
//
func IsColumnString(columnType *sql.ColumnType) bool {
	typeName := columnType.DatabaseTypeName()
	return strings.Contains(typeName, "TEXT") ||
		strings.Contains(typeName, "CHAR") ||
		strings.Contains(typeName, "JSON")
}

// float, double is already handled
// https://github.com/go-sql-driver/mysql/blob/master/fields.go#L166
func IsColumnFloat(columnType *sql.ColumnType) bool {
	typeName := columnType.DatabaseTypeName()
	return strings.Contains(typeName, "DECIMAL")
}

var nullString = reflect.TypeOf(sql.NullString{})

// GetScanType returns better scan type than go-sql-driver/mysql
func GetScanType(columnType *sql.ColumnType) reflect.Type {
	if IsColumnString(columnType) {
		return reflect.TypeOf(sql.NullString{})
	} else if IsColumnFloat(columnType) {
		return reflect.TypeOf(sql.NullFloat64{})
	} else {
		return columnType.ScanType()
	}
}

func GetScanPtrSafe(columnIdx int, columnTypes []*sql.ColumnType, vPtrs []interface{}) (interface{}, error) {
	scanType := GetScanType(columnTypes[columnIdx])
	if scanType.String() == "sql.RawBytes" {
		data := reflect.ValueOf(vPtrs[columnIdx]).Elem().Interface()
		dataRawBytes, ok := data.(sql.RawBytes)
		if !ok {
			return nil, errors.Errorf("[GetScanPtrSafe] failed to convert sql.RawBytes")
		}
		var b sql.RawBytes
		if dataRawBytes != nil {
			b = make(sql.RawBytes, len(dataRawBytes))
			copy(b, dataRawBytes)
		}
		return &b, nil
	} else {
		return vPtrs[columnIdx], nil
	}
}

func ScanGeneralRowsWithDataPtrs(rows *sql.Rows, columnTypes []*sql.ColumnType, vPtrs []interface{}) ([]interface{}, error) {
	if err := rows.Scan(vPtrs...); err != nil {
		return nil, errors.Trace(err)
	}
	// copy sql.RawBytes from db to here
	for i := range columnTypes {
		p, err := GetScanPtrSafe(i, columnTypes, vPtrs)
		if err != nil {
			return nil, errors.Trace(err)
		}
		vPtrs[i] = p
	}
	return vPtrs, nil
}

// func ScanGeneraRowWithDataPtrs(row *sql.Row, columnTypes []*sql.ColumnType, vPtrs []interface{}) ([]interface{}, bool, error) {
// 	if err := row.Scan(vPtrs...); err != nil {
// 		if err == sql.ErrNoRows {
// 			return nil, false, nil
// 		}
// 		return nil, false, errors.Trace(err)
// 	}
//
// 	// copy sql.RawBytes from db to here
// 	for i, _ := range columnTypes {
// 		p, err := GetScanPtrSafe(i, columnTypes, vPtrs)
// 		if err != nil {
// 			return nil, true, errors.Trace(err)
// 		}
// 		vPtrs[i] = p
// 	}
// 	return vPtrs, true, nil
// }

func ScanGeneralRows(rows *sql.Rows, columnTypes []*sql.ColumnType) ([]interface{}, error) {
	vPtrs := make([]interface{}, len(columnTypes))

	for i := range columnTypes {
		scanType := GetScanType(columnTypes[i])
		vptr := reflect.New(scanType)
		vPtrs[i] = vptr.Interface()
	}
	if err := rows.Scan(vPtrs...); err != nil {
		return nil, errors.Trace(err)
	}

	// copy sql.RawBytes from db to here
	for i := range columnTypes {
		p, err := GetScanPtrSafe(i, columnTypes, vPtrs)
		if err != nil {
			return nil, errors.Trace(err)
		}
		vPtrs[i] = p
	}
	return vPtrs, nil
}

func QueryGeneralRowsDataWithSQL(db *sql.DB, statement string, args ...interface{}) ([][]interface{}, error) {

	rows, err := db.Query(statement, args...)
	if err != nil {
		return nil, errors.Trace(err)
	}

	defer func() {
		err = rows.Close()
		if err != nil {
			log.Fatalf("close error")
		}

		err = rows.Err()
		if err != nil {
			log.Fatalf("rows error")
		}
	}()

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, errors.Trace(err)
	}

	var rowsPtr [][]interface{}
	for rows.Next() {
		vPtrs, err := ScanGeneralRows(rows, columnTypes)
		if err != nil {
			return nil, errors.Annotatef(err, "stmt: %s. args: %s", statement, args)
		}
		rowsPtr = append(rowsPtr, vPtrs)
	}
	return rowsPtr, nil
}

func CreateDBConnection(cfg *config.DBConfig) (*sql.DB, error) {
	if err := cfg.ValidateAndSetDefault(); err != nil {
		return nil, errors.Trace(err)
	}

	dbDSN := fmt.Sprintf(`%s:%s@tcp(%s:%d)/%s?interpolateParams=true&timeout=%s&readTimeout=%s&writeTimeout=%s&parseTime=true&collation=utf8mb4_general_ci`,
		cfg.Username, cfg.Password, cfg.Host, cfg.Port, url.QueryEscape(cfg.Schema), cfg.Timeout, cfg.ReadTimeout, cfg.WriteTimeout)
	if cfg.Location != "" {
		dbDSN += "&loc=" + url.QueryEscape(cfg.Location)
	}
	log.Infof("DSN is %s", dbDSN)
	db, err := sql.Open("mysql", dbDSN)
	if err != nil {
		return nil, errors.Trace(err)
	}

	err = db.Ping()
	if err != nil {
		return nil, errors.Trace(err)
	}

	db.SetMaxOpenConns(cfg.MaxOpen)
	db.SetMaxIdleConns(cfg.MaxIdle)
	db.SetConnMaxLifetime(cfg.MaxLifeTimeDuration)
	return db, nil
}

func CloseDBConnection(db *sql.DB) error {
	if db == nil {
		return nil
	}

	return errors.Trace(db.Close())
}

func CloseDBConnections(dbs ...*sql.DB) {
	for _, db := range dbs {
		if err := CloseDBConnection(db); err != nil {
			log.Errorf("close db failed: %v", err)
		}
	}
}

func TestConfig() *config.DBConfig {
	host, ok := os.LookupEnv("DB_HOST")
	if !ok {
		host = "localhost"
	}
	port, ok := os.LookupEnv("DB_PORT")
	if !ok {
		port = "3306"
	}
	user, ok := os.LookupEnv("DB_USER")
	if !ok {
		user = "root"
	}
	pwd, ok := os.LookupEnv("DB_PWD")
	if !ok {
		pwd = ""
	}
	schema, ok := os.LookupEnv("DB_SCHEMA")
	if !ok {
		schema = ""
	}

	iport, _ := strconv.Atoi(port)

	return &config.DBConfig{
		Host:     host,
		Port:     iport,
		Username: user,
		Password: pwd,
		Schema:   schema,
	}
}

// CheckBinlogFormat checks binlog format
func CheckBinlogFormat(db *sql.DB) error {
	rows, err := db.Query(`SHOW GLOBAL VARIABLES LIKE "binlog_format";`)
	if err != nil {
		return errors.Trace(err)
	}
	defer rows.Close()

	// Show an example.
	/*
			   mysql> SHOW GLOBAL VARIABLES LIKE "binlog_format";
		       +---------------+-------+
		       | Variable_name | Value |
		       +---------------+-------+
		       | binlog_format | ROW   |
		       +---------------+-------+
	*/
	for rows.Next() {
		var (
			variable string
			value    string
		)

		err = rows.Scan(&variable, &value)

		if err != nil {
			return errors.Trace(err)

		}

		if variable == "binlog_format" && value != "ROW" {
			log.Fatalf("binlog_format is not 'ROW': %v", value)
		}
	}

	if rows.Err() != nil {
		return errors.Trace(rows.Err())
	}

	// make sure binlog_row_image is FULL
	binlogRowImageRows, err := db.Query(`SHOW GLOBAL VARIABLES LIKE 'binlog_row_image';`)
	if err != nil {
		return errors.Trace(err)
	}
	defer binlogRowImageRows.Close()

	for binlogRowImageRows.Next() {
		var (
			variable string
			value    string
		)

		err = binlogRowImageRows.Scan(&variable, &value)
		if err != nil {
			return errors.Trace(err)
		}

		if variable == "binlog_row_image" && value != "FULL" {
			log.Fatalf("binlog_row_image is not 'FULL' : %v", value)
		}
	}

	if binlogRowImageRows.Err() != nil {
		return errors.Trace(binlogRowImageRows.Err())
	}

	return nil
}

type MySQLStatusGetter interface {
	GetMasterStatus() (gomysql.Position, gomysql.MysqlGTIDSet, error)
	GetServerUUID() (string, error)
}

type MySQLDB struct {
	*sql.DB
}

func NewMySQLDB(db *sql.DB) MySQLDB {
	return MySQLDB{db}
}

func (db MySQLDB) GetMasterStatus() (gomysql.Position, gomysql.MysqlGTIDSet, error) {
	var (
		binlogPos gomysql.Position
		gs        gomysql.MysqlGTIDSet
	)
	rows, err := db.Query(`SHOW MASTER STATUS`)
	if err != nil {
		return binlogPos, gs, errors.Trace(err)
	}
	defer rows.Close()

	rowColumns, err := rows.Columns()
	if err != nil {
		return binlogPos, gs, errors.Trace(err)
	}

	var (
		gtid       string
		binlogName string
		pos        uint32
		nullPtr    interface{}
	)
	for rows.Next() {
		if len(rowColumns) == 5 {
			err = rows.Scan(&binlogName, &pos, &nullPtr, &nullPtr, &gtid)
		} else {
			err = rows.Scan(&binlogName, &pos, &nullPtr, &nullPtr)
		}
		if err != nil {
			return binlogPos, gs, errors.Trace(err)
		}

		binlogPos = gomysql.Position{Name: binlogName, Pos: pos}
		generalGTIDSet, err := gomysql.ParseMysqlGTIDSet(gtid)
		if err != nil {
			return binlogPos, gs, errors.Trace(err)
		}
		gs = *(generalGTIDSet.(*gomysql.MysqlGTIDSet))
	}
	if rows.Err() != nil {
		return binlogPos, gs, errors.Trace(rows.Err())
	}

	return binlogPos, gs, nil
}

func (db MySQLDB) GetServerUUID() (string, error) {
	var masterUUID string
	rows, err := db.Query(`select @@server_uuid;`)
	if err != nil {
		return "", errors.Trace(err)
	}
	defer rows.Close()

	// Show an example.
	/*
	   MySQL [test]> SHOW MASTER STATUS;
	   +--------------------------------------+
	   | @@server_uuid                        |
	   +--------------------------------------+
	   | 53ea0ed1-9bf8-11e6-8bea-64006a897c73 |
	   +--------------------------------------+
	*/
	for rows.Next() {
		err = rows.Scan(&masterUUID)
		if err != nil {
			return "", errors.Trace(err)
		}
	}
	if rows.Err() != nil {
		return "", errors.Trace(rows.Err())
	}
	return masterUUID, nil
}

// CompareBinlogPos Compare binlog positions.
// The result will be 0 if |a-b| < deviation, otherwise -1 if a < b, and +1 if a > b.
func CompareBinlogPos(a, b gomysql.Position, deviation float64) int {
	if a.Name < b.Name {
		return -1
	}

	if a.Name > b.Name {
		return 1
	}

	if math.Abs(float64(a.Pos-b.Pos)) <= deviation {
		return 0
	}

	if a.Pos < b.Pos {
		return -1
	}

	return 1
}

func GenerateRandomServerID() uint32 {
	s := rand.NewSource(time.Now().UnixNano())
	r := rand.New(s)
	for {
		id := r.Uint32() % 10000000
		if id > 0 {
			return id
		}
	}
}

// GetServerTimezone get timezone of the MySQL server.
func GetServerTimezone(db *sql.DB) (string, error) {
	var tz string
	rows, err := db.Query(`SELECT timediff(now(), UTC_TIMESTAMP);`)
	if err != nil {
		return "", errors.Trace(err)
	}
	defer rows.Close()

	// Show server timezone.
	/*
	   MySQL [test]> select timediff(now(), UTC_TIMESTAMP);
	   +--------------------------------------+
	   | time_zone                            |
	   +--------------------------------------+
	   | 08:00:00                             |
	   +--------------------------------------+
	*/
	for rows.Next() {
		err = rows.Scan(&tz)
		if err != nil {
			return "", errors.Trace(err)
		}
	}
	if rows.Err() != nil {
		return "", errors.Trace(rows.Err())
	}
	return tz, nil
}

func IsBinlogPurgedError(err error) bool {
	mysqlErr, ok := errors.Cause(err).(*gomysql.MyError)
	if !ok {
		return false
	}
	errCode := uint16(mysqlErr.Code)
	if errCode == gomysql.ER_MASTER_FATAL_ERROR_READING_BINLOG {
		return true
	}
	return false
}

func TableIdentity(schemaName string, tableName string) string {
	return fmt.Sprintf("`%s`.`%s`", schemaName, tableName)
}

func GetTables(db *sql.DB) ([]string, error) {
	rows, err := db.Query("SELECT TABLE_SCHEMA, TABLE_NAME FROM information_schema.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA NOT IN ('mysql','performance_schema')")
	if err != nil {
		return nil, errors.Annotatef(err, "error GetTables")
	}
	defer rows.Close()

	var tableIdentities []string
	for rows.Next() {
		var schema string
		var table string
		err = rows.Scan(&schema, &table)
		if err != nil {
			return nil, errors.Annotatef(err, "error GetTables")
		}

		tableIdentities = append(tableIdentities, TableIdentity(schema, table))
	}
	err = rows.Err()
	if err != nil {
		return nil, errors.Annotatef(err, "error GetTables")
	}

	return tableIdentities, nil
}

func SQLWithAnnotation(annotation string, sql string) string {
	return fmt.Sprintf("%s%s", annotation, sql)
}

func NewBinlogSyncer(serverID uint32, dbConfig *config.DBConfig, heartbeatPeriod time.Duration) *replication.BinlogSyncer {
	syncerConfig := replication.BinlogSyncerConfig{
		ServerID:  serverID,
		Flavor:    "mysql",
		Host:      dbConfig.Host,
		Port:      uint16(dbConfig.Port),
		User:      dbConfig.Username,
		Password:  dbConfig.Password,
		ParseTime: true,
	}

	if heartbeatPeriod > 0 {
		syncerConfig.HeartbeatPeriod = heartbeatPeriod
	}

	return replication.NewBinlogSyncer(syncerConfig)
}

func IsTiDB(db *sql.DB) bool {
	r := db.QueryRow("select version()")
	var s string
	err := r.Scan(&s)
	if err != nil {
		log.Panic(err)
	}
	return strings.Contains(strings.ToLower(s), "tidb")
}
