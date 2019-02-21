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
		result = append(result, r[4].String)
	}
	return result, nil
}

func IsTableEmpty(db *sql.DB, schema string, table string) bool {
	r := db.QueryRow(fmt.Sprintf("select count(*) from (select * from `%s`.`%s` limit 1)t", schema, table))
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
			+-------------+------------+----------+--------------+---------+
		| Table       | Non_unique | Key_name | Seq_in_index | Column_name |...
		+-------------+------------+----------+--------------+-------------+
		| luoxia_0003 |          0 | PRIMARY  |            1 | uid         |...
		+-------------+------------+----------+--------------+-------------+
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
	statement := fmt.Sprintf("SHOW INDEX FROM `%s`.`%s` WHERE Key_name = 'PRIMARY'", schemaName, tableName)
	return getIndexRowsName(db, statement)
}

func GetUniqueIndexesWithoutPks(db *sql.DB, schemaName string, tableName string) ([]string, error) {
	statement := fmt.Sprintf("SHOW INDEX FROM `%s`.`%s` WHERE Key_name != 'PRIMARY' AND Non_unique = 0", schemaName, tableName)
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

func IsColumnString(columnType *sql.ColumnType) bool {
	return strings.Contains(columnType.DatabaseTypeName(), "TEXT") || strings.Contains(columnType.DatabaseTypeName(), "CHAR")
	// if columnDatabaseType == "TEXT" ||
	// 	columnDatabaseType == "JSON" ||
	// 	columnDatabaseType == "LONGTEXT" ||
	// 	columnDatabaseType == "MEDIUMTEXT" ||
	// 	columnDatabaseType == "CHAR" ||
	// 	columnDatabaseType == "TINYTEXT" ||
	// 	columnDatabaseType == "VARCHAR" {
	// 		return true
	// } else {
	// 	return false
	// }
}

var nullString = reflect.TypeOf(sql.NullString{})

// GetScanType returns better scan type than go-sql-driver/mysql
func GetScanType(columnType *sql.ColumnType) reflect.Type {
	if IsColumnString(columnType) {
		return reflect.TypeOf(sql.NullString{})
	} else if columnType.DatabaseTypeName() == "DECIMAL" {
		return nullString
	} else if columnType.DatabaseTypeName() == "BIGINT" { // go-mysql can't handle unsigned nullable bigint
		return nullString
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
	for i, _ := range columnTypes {
		p, err := GetScanPtrSafe(i, columnTypes, vPtrs)
		if err != nil {
			return nil, errors.Trace(err)
		}
		vPtrs[i] = p
	}
	return vPtrs, nil
}

func ScanGeneralRows(rows *sql.Rows, columnTypes []*sql.ColumnType) ([]interface{}, error) {
	vPtrs := make([]interface{}, len(columnTypes))

	for i, _ := range columnTypes {
		scanType := GetScanType(columnTypes[i])
		vptr := reflect.New(scanType)
		vPtrs[i] = vptr.Interface()
	}
	if err := rows.Scan(vPtrs...); err != nil {
		return nil, errors.Trace(err)
	}

	// copy sql.RawBytes from db to here
	for i, _ := range columnTypes {
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

func CreateDBConnection(cfg *DBConfig) (*sql.DB, error) {
	if err := cfg.ValidateAndSetDefault(); err != nil {
		return nil, errors.Trace(err)
	}

	dbDSN := fmt.Sprintf(`%s:%s@tcp(%s:%d)/%s?interpolateParams=true&readTimeout=%s&parseTime=true&collation=utf8mb4_general_ci`,
		cfg.Username, cfg.Password, cfg.Host, cfg.Port, url.QueryEscape(cfg.Schema), "30s")
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

// DBConfig is the DB configuration.
type DBConfig struct {
	Host                   string        `toml:"host" json:"host" mapstructure:"host"`
	Location               string        `toml:"location" json:"location" mapstructure:"location"`
	Username               string        `toml:"username" json:"username" mapstructure:"username"`
	Password               string        `toml:"password" json:"password" mapstructure:"password"`
	Port                   int           `toml:"port" json:"port" mapstructure:"port"`
	Schema                 string        `toml:"schema" json:"schema" mapstructure:"schema"`
	MaxIdle                int           `toml:"max-idle" json:"max-idle" mapstructure:"max-idle"`
	MaxOpen                int           `toml:"max-open" json:"max-open" mapstructure:"max-open"`
	MaxLifeTimeDurationStr string        `toml:"max-life-time-duration" json:"max-life-time-duration" mapstructure:"max-life-time-duration"`
	MaxLifeTimeDuration    time.Duration `toml:"-" json:"-" mapstructure:"-"`
}

func (dbc *DBConfig) ValidateAndSetDefault() error {
	// Sets the location for time.Time values (when using parseTime=true). "Local" sets the system's location. See time.LoadLocation for details.
	// Note that this sets the location for time.Time values but does not change MySQL's time_zone setting.
	// For that see the time_zone system variable, which can also be set as a DSN parameter.
	if dbc.Location == "" {
		dbc.Location = time.Local.String()
	}

	// set default values of connection related settings
	// assume the response time of db is 2ms, then
	// then a single connection can have tps of 500 TPS
	if dbc.MaxOpen == 0 {
		dbc.MaxOpen = 200
	}

	if dbc.MaxIdle == 0 {
		dbc.MaxIdle = dbc.MaxOpen
	}

	var err error
	if dbc.MaxLifeTimeDurationStr == "" {
		dbc.MaxLifeTimeDurationStr = "15m"
		dbc.MaxLifeTimeDuration = 15 * time.Minute
	} else {
		dbc.MaxLifeTimeDuration, err = time.ParseDuration(dbc.MaxLifeTimeDurationStr)
		if err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

func TestConfig() *DBConfig {
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

	return &DBConfig{
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

type MySQLBinlogPosition struct {
	BinLogFileName string `toml:"binlog-name" json:"binlog-name" mapstructure:"binlog-name"`
	BinLogFilePos  uint32 `toml:"binlog-pos" json:"binlog-pos" mapstructure:"binlog-pos"`
	BinlogGTID     string `toml:"binlog-gtid" json:"binlog-gtid" mapstructure:"binlog-gtid"`
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

func NewBinlogSyncer(serverID uint32, dbConfig *DBConfig) *replication.BinlogSyncer {
	syncerConfig := replication.BinlogSyncerConfig{
		ServerID:  serverID,
		Flavor:    "mysql",
		Host:      dbConfig.Host,
		Port:      uint16(dbConfig.Port),
		User:      dbConfig.Username,
		Password:  dbConfig.Password,
		ParseTime: true,
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
