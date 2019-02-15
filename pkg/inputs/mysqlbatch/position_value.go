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
	PlainBytes    = "bytes"
	PlainTime     = "time"
	SQLNullInt64  = "sqlNullInt64"
	SQLNullString = "sqlNullString"
	SQLNullBool   = "sqlNullBool"
	SQLNullTime   = "sqlNullTime"
	SQLRawBytes   = "sqlRawBytes"
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
	case int:
		pMapString["value"] = strconv.FormatInt(int64(v), 10)
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
		v, err := strconv.Atoi(pMapString["value"])
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
		v, err := strconv.Atoi(pMapString["value"])
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
}

type BatchPositionValue struct {
	Start       *utils.MySQLBinlogPosition `toml:"start-binlog" json:"start-binlog"`
	TableStates map[string]TableStats      `toml:"table-stats" json:"table-stats"`
}

func EncodeBatchPositionValue(v interface{}) (string, error) {
	return myJson.MarshalToString(v)
}

func DecodeBatchPositionValue(s string) (interface{}, error) {
	v := BatchPositionValue{}
	if err := myJson.UnmarshalFromString(s, &v); err != nil {
		return nil, errors.Trace(err)
	}
	return &v, nil
}

func SetupInitialPosition(cache position_store.PositionCacheInterface, sourceDB *sql.DB) error {
	_, exist, err := cache.Get()
	if err != nil {
		return errors.Trace(err)
	}

	if !exist {
		dbUtil := utils.NewMySQLDB(sourceDB)
		binlogFilePos, gtid, err := dbUtil.GetMasterStatus()
		if err != nil {
			return errors.Trace(err)
		}

		startPosition := utils.MySQLBinlogPosition{
			BinLogFileName: binlogFilePos.Name,
			BinLogFilePos:  binlogFilePos.Pos,
			BinlogGTID:     gtid.String(),
		}

		batchPositionValue := BatchPositionValue{
			Start:       &startPosition,
			TableStates: make(map[string]TableStats),
		}
		position := position_store.Position{}
		position.Value = &batchPositionValue
		position.Stage = config.Batch
		position.UpdateTime = time.Now()
		if err := cache.Put(&position); err != nil {
			return errors.Trace(err)
		}
		if err := cache.Flush(); err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

var mu sync.Mutex

func GetStartBinlog(cache position_store.PositionCacheInterface) (*utils.MySQLBinlogPosition, error) {
	mu.Lock()
	defer mu.Unlock()

	position, exist, err := cache.Get()
	if err != nil {
		return nil, errors.Trace(err)
	}

	if !exist {
		return nil, errors.Errorf("empty position")
	}

	batchPositionValue, ok := position.Value.(*BatchPositionValue)
	if !ok {
		return nil, errors.Errorf("invalid position type")
	}

	return batchPositionValue.Start, nil
}

func GetCurrentPos(cache position_store.PositionCacheInterface, fullTableName string) (*TablePosition, bool, error) {
	mu.Lock()
	defer mu.Unlock()

	position, exist, err := cache.Get()
	if err != nil {
		return nil, false, errors.Trace(err)
	}

	if !exist {
		return nil, false, nil
	}

	batchPositionValue, ok := position.Value.(*BatchPositionValue)
	if !ok {
		return nil, true, errors.Errorf("invalid position type")
	}

	stats, ok := batchPositionValue.TableStates[fullTableName]
	if !ok {
		return nil, false, errors.Errorf("empty stats for table: %v", fullTableName)
	}

	if stats.Current != nil {
		return stats.Current, true, nil
	} else {
		return nil, false, nil
	}
}

func PutCurrentPos(cache position_store.PositionCacheInterface, fullTableName string, pos *TablePosition, incScanCount bool) error {
	mu.Lock()
	defer mu.Unlock()

	position, exist, err := cache.Get()
	if err != nil {
		return errors.Trace(err)
	}

	if !exist {
		return errors.Errorf("empty position")
	}

	batchPositionValue, ok := position.Value.(*BatchPositionValue)
	if !ok {
		return errors.Errorf("invalid position type")
	}

	stats, ok := batchPositionValue.TableStates[fullTableName]
	if !ok {
		return errors.Errorf("empty stats for table: %v", fullTableName)
	}

	stats.Current = pos
	if incScanCount {
		stats.ScannedCount++
	}
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

	batchPositionValue, ok := position.Value.(*BatchPositionValue)
	if !ok {
		return errors.Errorf("invalid position type")
	}

	stats, ok := batchPositionValue.TableStates[fullTableName]
	if !ok {
		batchPositionValue.TableStates[fullTableName] = TableStats{}
		stats = batchPositionValue.TableStates[fullTableName]
	}

	stats.EstimatedRowCount = estimatedCount
	batchPositionValue.TableStates[fullTableName] = stats

	position.Value = batchPositionValue
	return errors.Trace(cache.Put(position))
}

func GetMaxMin(cache position_store.PositionCacheInterface, fullTableName string) (*TablePosition, *TablePosition, bool, error) {
	mu.Lock()
	defer mu.Unlock()

	position, exist, err := cache.Get()
	if err != nil {
		return nil, nil, false, errors.Trace(err)
	}

	if !exist {
		return nil, nil, false, nil
	}

	batchPositionValue, ok := position.Value.(*BatchPositionValue)
	if !ok {
		return nil, nil, true, errors.Errorf("invalid position type")
	}

	stats, ok := batchPositionValue.TableStates[fullTableName]
	if !ok {
		return nil, nil, false, nil
	}

	return stats.Max, stats.Min, stats.Min != nil && stats.Max != nil, nil
}

func PutMaxMin(cache position_store.PositionCacheInterface, fullTableName string, max *TablePosition, min *TablePosition) error {
	mu.Lock()
	defer mu.Unlock()

	position, exist, err := cache.Get()
	if err != nil {
		return errors.Trace(err)
	}

	if !exist {
		return errors.Errorf("empty position")
	}

	batchPositionValue, ok := position.Value.(*BatchPositionValue)
	if !ok {
		return errors.Errorf("invalid position type")
	}

	stats, ok := batchPositionValue.TableStates[fullTableName]
	if !ok {
		batchPositionValue.TableStates[fullTableName] = TableStats{}
		stats = batchPositionValue.TableStates[fullTableName]
	}
	stats.Max = max
	stats.Min = min
	batchPositionValue.TableStates[fullTableName] = stats

	position.Value = batchPositionValue

	return errors.Trace(cache.Put(position))
}
