package mysqlbatch

import (
	"database/sql"
	"reflect"
	"strconv"
	"time"

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
	SQLNullInt64  = "sqlNullInt64"
	SQLNullString = "sqlNullString"
	SQLNullBool   = "sqlNullBool"
	SQLNullTime   = "sqlNullTime"
	SQLRawBytes   = "sqlRawBytes"
)

var myJson = jsoniter.Config{SortMapKeys: true}.Froze()

func isPositionEquals(p1 *utils.MySQLBinlogPosition, p2 *utils.MySQLBinlogPosition) bool {
	return p1.BinlogGTID == p2.BinlogGTID
}

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
			pMapString["value"] = v.Time.String()
			pMapString["type"] = SQLNullTime
		}
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
		t, err := time.Parse(time.RFC3339, pMapString["value"])
		if err != nil {
			return errors.Trace(err)
		}
		p.Value = t
	default:
		return errors.Errorf("[UnmarshalJSON] unknown type: %v, column: %v", p.Type, p.Column)
	}
	return nil
}

type BatchPositionValue struct {
	Start   *utils.MySQLBinlogPosition `toml:"start-binlog" json:"start-binlog"`
	Min     map[string]TablePosition   `toml:"min" json:"min"`
	Max     map[string]TablePosition   `toml:"max" json:"max"`
	Current map[string]TablePosition   `toml:"current" json:"current"`
}

func Serialize(positions *BatchPositionValue) (string, error) {
	s, err := myJson.MarshalToString(positions)
	if err != nil {
		return "", errors.Trace(err)
	}
	return s, nil
}

func Deserialize(value string) (*BatchPositionValue, error) {
	positions := BatchPositionValue{}
	if err := myJson.UnmarshalFromString(value, &positions); err != nil {
		return nil, errors.Trace(err)
	}
	return &positions, nil
}

func InitPositionCache(cache position_store.PositionCacheInterface, sourceDB *sql.DB) error {
	position := cache.Get()
	batchPositions, err := Deserialize(position.Value)
	if err != nil {
		return errors.Trace(err)
	}

	if batchPositions.Start != nil {
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
		batchPositions.Start = &startPosition

		v, err := Serialize(batchPositions)
		if err != nil {
			return errors.Trace(err)
		}
		position.Value = v
		cache.Put(position)
		if err := cache.Flush(); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func GetStartBinlog(cache position_store.PositionCacheInterface) (*utils.MySQLBinlogPosition, error) {
	position := cache.Get()
	batchPositions, err := Deserialize(position.Value)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return batchPositions.Start, nil
}

func GetCurrentPos(cache position_store.PositionCacheInterface, fullTableName string) (*TablePosition, bool, error) {
	position := cache.Get()
	batchPositions, err := Deserialize(position.Value)
	if err != nil {
		return nil, false, errors.Trace(err)
	}

	current, ok := batchPositions.Current[fullTableName]
	if !ok {
		return nil, false, nil
	}
	return &current, true, nil
}

func PutCurrentPos(cache position_store.PositionCacheInterface, fullTableName string, pos *TablePosition) error {
	position := cache.Get()
	batchPositions, err := Deserialize(position.Value)
	if err != nil {
		return errors.Trace(err)
	}

	batchPositions.Current[fullTableName] = *pos
	v, err := Serialize(batchPositions)
	if err != nil {
		return errors.Trace(err)
	}
	position.Value = v
	cache.Put(position)
	return nil
}

func GetMaxMin(cache position_store.PositionCacheInterface, fullTableName string) (*TablePosition, *TablePosition, bool, error) {
	position := cache.Get()
	batchPositions, err := Deserialize(position.Value)
	if err != nil {
		return nil, nil, false, errors.Trace(err)
	}

	max, ok := batchPositions.Max[fullTableName]
	if !ok {
		return nil, nil, false, nil
	}

	min, ok := batchPositions.Min[fullTableName]
	if !ok {
		return nil, nil, false, nil
	}

	return &max, &min, true, nil
}

func PutMaxMin(cache position_store.PositionCacheInterface, fullTableName string, max *TablePosition, min *TablePosition) error {
	position := cache.Get()
	batchPositions, err := Deserialize(position.Value)
	if err != nil {
		return errors.Trace(err)
	}

	batchPositions.Max[fullTableName] = *max
	batchPositions.Min[fullTableName] = *min

	cache.Put(position)
	return nil
}

// func (tablePositionState *BatchPositionValue) Get() interface{} {
// 	s := tablePositionState.GetRaw()
// 	ret := BatchPositionValue{}
// 	if err := myJson.UnmarshalFromString(s, &ret); err != nil {
// 		log.Fatalf("[BatchPositionValue.Get] err: %s", err)
// 	}
// 	return ret
// }
//
// func (tablePositionState *BatchPositionValue) GetRaw() string {
// 	ret, err := tablePositionState.ToJSON()
// 	if err != nil {
// 		log.Fatalf("[BatchPositionValue.GetRaw] error ToJSON. %#v. err: %v", tablePositionState, errors.ErrorStack(err))
// 	}
// 	return ret
// }
//
// func (tablePositionState *BatchPositionValue) PutRaw(pos string) {
// 	if pos == "" {
// 		*tablePositionState = BatchPositionValue{}
// 	} else {
// 		if err := myJson.UnmarshalFromString(pos, tablePositionState); err != nil {
// 			log.Fatalf("[BatchPositionValue.PutRaw] error put %s. err: %s", pos, err)
// 		}
// 	}
// }
//
// func (tablePositionState *BatchPositionValue) Put(pos interface{}) {
// 	*tablePositionState = pos.(BatchPositionValue)
// }
//
// func (tablePositionState *BatchPositionValue) Stage() config.InputMode {
// 	return config.Batch
// }
//
// func (tablePositionState *BatchPositionValue) ToJSON() (string, error) {
// 	tablePositionState.Lock()
// 	defer tablePositionState.Unlock()
// 	s, err := myJson.MarshalToString(tablePositionState)
// 	return s, errors.Trace(err)
// }
//
// func (tablePositionState *BatchPositionValue) GetStartBinlogPos() (utils.MySQLBinlogPosition, bool) {
// 	tablePositionState.Lock()
// 	defer tablePositionState.Unlock()
//
// 	if tablePositionState.Start == nil {
// 		return utils.MySQLBinlogPosition{}, false
// 	}
//
// 	return *tablePositionState.Start, true
// }
//
// func (tablePositionState *BatchPositionValue) PutStartBinlogPos(p utils.MySQLBinlogPosition) {
// 	tablePositionState.Lock()
// 	defer tablePositionState.Unlock()
//
// 	tablePositionState.Start = &p
// }
//
// func (tablePositionState *BatchPositionValue) GetMaxMin(sourceName string) (TablePosition, TablePosition, bool) {
// 	log.Infof("[tablePositionState] GetMaxMin")
// 	tablePositionState.Lock()
// 	defer tablePositionState.Unlock()
//
// 	if tablePositionState.Min == nil || tablePositionState.Max == nil {
// 		return TablePosition{}, TablePosition{}, false
// 	}
//
// 	max, okMax := tablePositionState.Max[sourceName]
//
// 	min, okMin := tablePositionState.Min[sourceName]
// 	return max, min, okMax && okMin
// }
//
// func (tablePositionState *BatchPositionValue) PutMaxMin(sourceName string, max TablePosition, min TablePosition) {
// 	tablePositionState.Lock()
// 	defer tablePositionState.Unlock()
//
// 	if tablePositionState.Min == nil {
// 		tablePositionState.Min = make(map[string]TablePosition)
// 	}
//
// 	if tablePositionState.Max == nil {
// 		tablePositionState.Max = make(map[string]TablePosition)
// 	}
// 	tablePositionState.Max[sourceName] = max
// 	tablePositionState.Min[sourceName] = min
// }
//
// func (tablePositionState *BatchPositionValue) GetCurrent(sourceName string) (TablePosition, bool) {
// 	tablePositionState.Lock()
// 	defer tablePositionState.Unlock()
//
// 	if tablePositionState.Current == nil {
// 		return TablePosition{}, false
// 	}
//
// 	p, ok := tablePositionState.Current[sourceName]
// 	return p, ok
// }
//
// func (tablePositionState *BatchPositionValue) PutCurrent(sourceName string, pos TablePosition) {
// 	tablePositionState.Lock()
// 	defer tablePositionState.Unlock()
//
// 	// log.Infof("[LoopInBatch] PutCurrent: sourceName: %v %v", sourceName, pos)
//
// 	if tablePositionState.Current == nil {
// 		tablePositionState.Current = make(map[string]TablePosition)
// 	}
//
// 	tablePositionState.Current[sourceName] = pos
// }
//
// func SerializeMySQLBinlogPosition(pos gomysql.Position, gtidSet gomysql.MysqlGTIDSet) utils.MySQLBinlogPosition {
// 	p := utils.MySQLBinlogPosition{}
// 	p.BinLogFileName = pos.Name
// 	p.BinLogFilePos = pos.Pos
// 	p.BinlogGTID = gtidSet.String()
// 	return p
// }
//
// func DeserializeMySQLBinlogPosition(p utils.MySQLBinlogPosition) (gomysql.Position, gomysql.MysqlGTIDSet, error) {
// 	gtidSet, err := gomysql.ParseMysqlGTIDSet(p.BinlogGTID)
// 	if err != nil {
// 		return gomysql.Position{}, gomysql.MysqlGTIDSet{}, errors.Trace(err)
// 	}
//
// 	mysqlGTIDSet := *gtidSet.(*gomysql.MysqlGTIDSet)
// 	return gomysql.Position{Name: p.BinLogFileName, Pos: p.BinLogFilePos}, mysqlGTIDSet, nil
// }
