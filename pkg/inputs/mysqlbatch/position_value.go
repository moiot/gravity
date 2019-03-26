package mysqlbatch

import (
	"database/sql"
	"reflect"
	"strconv"
	"sync"
	"time"

	"github.com/moiot/gravity/pkg/config"

	"github.com/json-iterator/go"
	"github.com/moiot/gravity/pkg/position_store"

	"github.com/go-sql-driver/mysql"
	"github.com/juju/errors"
	log "github.com/sirupsen/logrus"

	"github.com/moiot/gravity/pkg/utils"
)

const (
	Unknown       = "*"
	PlainString   = "string"
	PlainInt      = "int"
	PlainUInt     = "uint"
	PlainBytes    = "bytes"
	PlainTime     = "time"
	SQLNullInt64  = "sqlNullInt64"
	SQLNullString = "sqlNullString"
	SQLNullBool   = "sqlNullBool"
	SQLNullTime   = "sqlNullTime"
	SQLRawBytes   = "sqlRawBytes"

	ScanColumnForDump = "*"

	SchemaVersionV1 = "v1.0"
)

var myJson = jsoniter.Config{SortMapKeys: true}.Froze()

type TablePosition struct {
	Value  interface{} `toml:"value" json:"value,omitempty"`
	Type   string      `toml:"type" json:"type"`
	Column string      `toml:"column" json:"column"`
}

func (p TablePosition) MapString() (map[string]string, error) {
	pMapString := make(map[string]string)
	pMapString["column"] = p.Column

	switch v := p.Value.(type) {
	case string:
		pMapString["value"] = v
		pMapString["type"] = PlainString
	case uint:
		pMapString["value"] = strconv.FormatUint(uint64(v), 10)
		pMapString["type"] = PlainUInt
	case uint8:
		pMapString["value"] = strconv.FormatUint(uint64(v), 10)
		pMapString["type"] = PlainUInt
	case uint16:
		pMapString["value"] = strconv.FormatUint(uint64(v), 10)
		pMapString["type"] = PlainUInt
	case uint32:
		pMapString["value"] = strconv.FormatUint(uint64(v), 10)
		pMapString["type"] = PlainUInt
	case uint64:
		pMapString["value"] = strconv.FormatUint(v, 10)
		pMapString["type"] = PlainUInt
	case int:
		pMapString["value"] = strconv.FormatInt(int64(v), 10)
		pMapString["type"] = PlainInt
	case int8:
		pMapString["value"] = strconv.FormatInt(int64(v), 10)
		pMapString["type"] = PlainInt
	case int16:
		pMapString["value"] = strconv.FormatInt(int64(v), 10)
		pMapString["type"] = PlainInt
	case int32:
		pMapString["value"] = strconv.FormatInt(int64(v), 10)
		pMapString["type"] = PlainInt
	case int64:
		pMapString["value"] = strconv.FormatInt(v, 10)
		pMapString["type"] = PlainInt
	case []byte:
		if v != nil {
			pMapString["value"] = string(v[:])
			pMapString["type"] = PlainBytes
		}
	case sql.RawBytes:
		if v != nil {
			pMapString["value"] = string(v[:])
			pMapString["type"] = SQLRawBytes
		}
	case sql.NullInt64:
		if v.Valid {
			pMapString["value"] = strconv.FormatInt(v.Int64, 10)
			pMapString["type"] = SQLNullInt64
		}
	case sql.NullString:
		if v.Valid {
			pMapString["value"] = v.String
			pMapString["type"] = SQLNullString
		}
	case sql.NullBool:
		if v.Valid {
			pMapString["value"] = strconv.FormatBool(v.Bool)
			pMapString["type"] = SQLNullBool
		}
	case sql.NullFloat64:
		log.Fatalf("not supported")

	case mysql.NullTime:
		if v.Valid {
			pMapString["value"] = v.Time.Format(time.RFC3339Nano)
			pMapString["type"] = SQLNullTime
		}
	case time.Time:
		pMapString["value"] = v.Format(time.RFC3339Nano)
		pMapString["type"] = SQLNullTime
	default:
		return nil, errors.Errorf("[MapString] unknown type: %v, column: %v", reflect.TypeOf(v).String(), p.Column)
	}
	return pMapString, nil
}

func (p TablePosition) MarshalJSON() ([]byte, error) {
	m, err := p.MapString()
	if err != nil {
		return nil, errors.Trace(err)
	}

	b, err := myJson.Marshal(m)
	if err != nil {
		return nil, errors.Annotatef(err, "[MarshalJSON] failed to marshal column: %v, type: %v, value: %v", p.Column, p.Type, p.Value)
	}
	return b, nil
}

func (p *TablePosition) UnmarshalJSON(value []byte) error {
	pMapString := make(map[string]string)

	if err := myJson.Unmarshal(value, &pMapString); err != nil {
		return errors.Trace(err)
	}

	p.Type = pMapString["type"]
	p.Column = pMapString["column"]
	switch p.Type {
	case PlainString:
		p.Value = pMapString["value"]
	case PlainInt:
		v, err := strconv.ParseInt(pMapString["value"], 10, 64)
		if err != nil {
			return errors.Trace(err)
		}
		p.Value = v
	case PlainUInt:
		v, err := strconv.ParseUint(pMapString["value"], 10, 64)
		if err != nil {
			return errors.Trace(err)
		}
		p.Value = v
	case PlainBytes:
		p.Value = []byte(pMapString["value"])
	case SQLRawBytes:
		// s := []byte(pMapString["value"])
		p.Value = pMapString["value"]
	case SQLNullInt64:
		v, err := strconv.ParseInt(pMapString["value"], 10, 64)
		if err != nil {
			return errors.Trace(err)
		}
		p.Value = v
	case SQLNullString:
		p.Value = pMapString["value"]
	case SQLNullBool:
		b, err := strconv.ParseBool(pMapString["value"])
		if err != nil {
			return errors.Trace(err)
		}
		p.Value = b
	case SQLNullTime:
		t, err := time.Parse(time.RFC3339Nano, pMapString["value"])
		if err != nil {
			return errors.Trace(err)
		}
		p.Value = t
	default:
		return errors.Errorf("[UnmarshalJSON] unknown type: %v, column: %v", p.Type, p.Column)
	}
	return nil
}

type TableStats struct {
	Max               *TablePosition `toml:"max" json:"max"`
	Min               *TablePosition `toml:"min" json:"min"`
	Current           *TablePosition `toml:"current" json:"current"`
	EstimatedRowCount int64          `json:"estimated-count"`
	ScannedCount      int64          `json:"scanned-count"`
	Done              bool           `json:"done"`
}

type TableStatsV1 struct {
	Max               []TablePosition `toml:"max" json:"max"`
	Min               []TablePosition `toml:"min" json:"min"`
	Current           []TablePosition `toml:"current" json:"current"`
	EstimatedRowCount int64           `json:"estimated-count"`
	ScannedCount      int64           `json:"scanned-count"`
	Done              bool            `json:"done"`
}

type BatchPositionValueV1Beta1 struct {
	SchemaVersion string                    `toml:"schema-version" json:"schema-version"`
	Start         utils.MySQLBinlogPosition `toml:"start-binlog" json:"start-binlog"`
	TableStates   map[string]TableStats     `toml:"table-stats" json:"table-stats"`
}

type BatchPositionValueV1 struct {
	SchemaVersion string                    `toml:"schema-version" json:"schema-version"`
	Start         utils.MySQLBinlogPosition `toml:"start-binlog" json:"start-binlog"`
	TableStates   map[string]TableStatsV1   `toml:"table-stats" json:"table-stats"`
}

type BatchPositionVersionMigrationWrapper struct {
	SchemaVersion string `toml:"schema-version" json:"schema-version"`
}

func EncodeBatchPositionValue(v interface{}) (string, error) {
	v1, ok := v.(BatchPositionValueV1)
	if !ok {
		return "", errors.Errorf("invalid position value type: %v", reflect.TypeOf(v))
	}
	v1.SchemaVersion = SchemaVersionV1
	return myJson.MarshalToString(v1)
}

func DecodeBatchPositionValue(s string) (interface{}, error) {
	wrapper := BatchPositionVersionMigrationWrapper{}
	if err := myJson.UnmarshalFromString(s, &wrapper); err != nil {
		return nil, errors.Trace(err)
	}

	// migrate old version to SchemaVersionV1
	if wrapper.SchemaVersion == "" {
		v1beta1 := BatchPositionValueV1Beta1{}
		if err := myJson.UnmarshalFromString(s, &v1beta1); err != nil {
			return nil, errors.Trace(err)
		}

		v1 := BatchPositionValueV1{}
		v1.SchemaVersion = SchemaVersionV1
		v1.Start = v1beta1.Start
		v1.TableStates = make(map[string]TableStatsV1)

		for t := range v1beta1.TableStates {
			tableStats := TableStatsV1{}
			if v1beta1.TableStates[t].Max != nil {
				tableStats.Max = []TablePosition{*v1beta1.TableStates[t].Max}
			}

			if v1beta1.TableStates[t].Min != nil {
				tableStats.Min = []TablePosition{*v1beta1.TableStates[t].Min}
			}

			if v1beta1.TableStates[t].Current != nil {
				tableStats.Current = []TablePosition{*v1beta1.TableStates[t].Current}
			}

			tableStats.EstimatedRowCount = v1beta1.TableStates[t].EstimatedRowCount
			tableStats.ScannedCount = v1beta1.TableStates[t].ScannedCount
			tableStats.Done = v1beta1.TableStates[t].Done
			v1.TableStates[t] = tableStats
		}
		return v1, nil

	} else if wrapper.SchemaVersion == SchemaVersionV1 {
		v1 := BatchPositionValueV1{}
		if err := myJson.UnmarshalFromString(s, &v1); err != nil {
			return nil, errors.Trace(err)
		}
		return v1, nil
	}

	return nil, errors.Errorf("no valid version")
}

func SetupInitialPosition(cache position_store.PositionCacheInterface, sourceDB *sql.DB) error {
	_, exist, err := cache.Get()
	if err != nil {
		return errors.Trace(err)
	}

	if !exist {
		dbUtil := utils.NewMySQLDB(sourceDB)
		var startPosition utils.MySQLBinlogPosition

		// For tidb, we just assume the binlog position is empty
		if !utils.IsTiDB(sourceDB) {
			binlogFilePos, gtid, err := dbUtil.GetMasterStatus()
			if err != nil {
				return errors.Trace(err)
			}

			startPosition = utils.MySQLBinlogPosition{
				BinLogFileName: binlogFilePos.Name,
				BinLogFilePos:  binlogFilePos.Pos,
				BinlogGTID:     gtid.String(),
			}
		}

		batchPositionValue := BatchPositionValueV1{
			Start:       startPosition,
			TableStates: make(map[string]TableStatsV1),
		}
		position := position_store.Position{}
		position.Value = batchPositionValue
		position.Stage = config.Batch
		position.UpdateTime = time.Now()
		if err := cache.Put(position); err != nil {
			return errors.Trace(err)
		}
		if err := cache.Flush(); err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

var mu sync.Mutex

func GetStartBinlog(cache position_store.PositionCacheInterface) (utils.MySQLBinlogPosition, error) {
	mu.Lock()
	defer mu.Unlock()

	position, exist, err := cache.Get()
	if err != nil {
		return utils.MySQLBinlogPosition{}, errors.Trace(err)
	}

	if !exist {
		return utils.MySQLBinlogPosition{}, errors.Errorf("empty position")
	}

	batchPositionValue, ok := position.Value.(BatchPositionValueV1)
	if !ok {
		return utils.MySQLBinlogPosition{}, errors.Errorf("invalid position type")
	}

	return batchPositionValue.Start, nil
}

func GetCurrentPos(cache position_store.PositionCacheInterface, fullTableName string) ([]TablePosition, bool, bool, error) {
	mu.Lock()
	defer mu.Unlock()

	position, exist, err := cache.Get()
	if err != nil {
		return nil, false, false, errors.Trace(err)
	}

	if !exist {
		return nil, false, false, nil
	}

	batchPositionValue, ok := position.Value.(BatchPositionValueV1)
	if !ok {
		return nil, false, true, errors.Errorf("invalid position type")
	}

	stats, ok := batchPositionValue.TableStates[fullTableName]
	if !ok {
		return nil, false, false, errors.Errorf("empty stats for table: %v", fullTableName)
	}

	if len(stats.Current) > 0 {
		return append([]TablePosition(nil), stats.Current...), stats.Done, true, nil
	} else {
		return nil, false, false, nil
	}
}

func PutCurrentPos(cache position_store.PositionCacheInterface, fullTableName string, pos []TablePosition, incScanCount bool) error {
	mu.Lock()
	defer mu.Unlock()

	position, exist, err := cache.Get()
	if err != nil {
		return errors.Trace(err)
	}

	if !exist {
		return errors.Errorf("empty position")
	}

	batchPositionValue, ok := position.Value.(BatchPositionValueV1)
	if !ok {
		return errors.Errorf("invalid position type")
	}

	stats, ok := batchPositionValue.TableStates[fullTableName]
	if !ok {
		return errors.Errorf("empty stats for table: %v", fullTableName)
	}

	stats.Current = append([]TablePosition(nil), pos...)
	if incScanCount {
		stats.ScannedCount++
	}
	batchPositionValue.TableStates[fullTableName] = stats

	position.Value = batchPositionValue
	return errors.Trace(cache.Put(position))
}

func PutDone(cache position_store.PositionCacheInterface, fullTableName string) error {
	mu.Lock()
	defer mu.Unlock()

	position, exists, err := cache.Get()
	if err != nil {
		return errors.Trace(err)
	}

	if !exists {
		return errors.Errorf("empty position")
	}

	batchPositionValue, ok := position.Value.(BatchPositionValueV1)
	if !ok {
		return errors.Errorf("invalid position type")
	}

	stats, ok := batchPositionValue.TableStates[fullTableName]
	if !ok {
		return errors.Errorf("empty stats for table: %v", fullTableName)
	}

	stats.Done = true
	batchPositionValue.TableStates[fullTableName] = stats

	position.Value = batchPositionValue
	return errors.Trace(cache.Put(position))
}

func PutEstimatedCount(cache position_store.PositionCacheInterface, fullTableName string, estimatedCount int64) error {
	mu.Lock()
	defer mu.Unlock()

	position, exist, err := cache.Get()
	if err != nil {
		return errors.Trace(err)
	}

	if !exist {
		return errors.Errorf("empty position")
	}

	batchPositionValue, ok := position.Value.(BatchPositionValueV1)
	if !ok {
		return errors.Errorf("invalid position type")
	}

	stats, ok := batchPositionValue.TableStates[fullTableName]
	if !ok {
		batchPositionValue.TableStates[fullTableName] = TableStatsV1{}
		stats = batchPositionValue.TableStates[fullTableName]
	}

	stats.EstimatedRowCount = estimatedCount
	batchPositionValue.TableStates[fullTableName] = stats

	position.Value = batchPositionValue
	return errors.Trace(cache.Put(position))
}

func GetMaxMin(cache position_store.PositionCacheInterface, fullTableName string) ([]TablePosition, []TablePosition, bool, error) {
	mu.Lock()
	defer mu.Unlock()

	position, exist, err := cache.Get()
	if err != nil {
		return nil, nil, false, errors.Trace(err)
	}

	if !exist {
		return nil, nil, false, nil
	}

	batchPositionValue, ok := position.Value.(BatchPositionValueV1)
	if !ok {
		return nil, nil, true, errors.Errorf("invalid position type")
	}

	stats, ok := batchPositionValue.TableStates[fullTableName]
	if !ok {
		return nil, nil, false, nil
	}

	if stats.Max == nil || stats.Min == nil {
		return nil, nil, false, nil
	}

	return append([]TablePosition(nil), stats.Max...), append([]TablePosition(nil), stats.Min...), true, nil
}

func PutMaxMin(cache position_store.PositionCacheInterface, fullTableName string, max []TablePosition, min []TablePosition) error {
	mu.Lock()
	defer mu.Unlock()

	position, exist, err := cache.Get()
	if err != nil {
		return errors.Trace(err)
	}

	if !exist {
		return errors.Errorf("empty position")
	}

	batchPositionValue, ok := position.Value.(BatchPositionValueV1)
	if !ok {
		return errors.Errorf("invalid position type")
	}

	stats, ok := batchPositionValue.TableStates[fullTableName]
	if !ok {
		batchPositionValue.TableStates[fullTableName] = TableStatsV1{}
		stats = batchPositionValue.TableStates[fullTableName]
	}

	stats.Max = append([]TablePosition(nil), max...)
	stats.Min = append([]TablePosition(nil), min...)
	batchPositionValue.TableStates[fullTableName] = stats

	position.Value = batchPositionValue
	return errors.Trace(cache.Put(position))
}
