package position_store

import (
	"database/sql"
	"strconv"
	"sync"

	"github.com/moiot/gravity/gravity/inputs/stages"

	"github.com/go-sql-driver/mysql"
	"github.com/juju/errors"
	gomysql "github.com/siddontang/go-mysql/mysql"
	log "github.com/sirupsen/logrus"

	"reflect"
	"time"

	"github.com/moiot/gravity/pkg/utils"
)

const (
	PlainString   = "string"
	PlainInt      = "int"
	PlainBytes    = "bytes"
	SQLNullInt64  = "sqlNullInt64"
	SQLNullString = "sqlNullString"
	SQLNullBool   = "sqlNullBool"
	SQLNullTime   = "sqlNullTime"
	SQLRawBytes   = "sqlRawBytes"
)

func isPositionEquals(p1 *utils.MySQLBinlogPosition, p2 *utils.MySQLBinlogPosition) bool {
	return p1.BinlogGTID == p2.BinlogGTID
}

type MySQLTablePosition struct {
	Value  interface{} `toml:"value" json:"value,omitempty"`
	Type   string      `toml:"type" json:"type"`
	Column string      `toml:"column" json:"column"`
}

func (p MySQLTablePosition) MapString() (map[string]string, error) {
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
		return nil, errors.Errorf("unknown type: %v", reflect.TypeOf(v))
	}
	return pMapString, nil
}

func (p MySQLTablePosition) MarshalJSON() ([]byte, error) {
	if m, err := p.MapString(); err != nil {
		return nil, errors.Trace(err)
	} else {
		return myJson.Marshal(m)
	}
}

func (p *MySQLTablePosition) UnmarshalJSON(value []byte) error {
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
		return errors.Errorf("unknown type: %v", p.Type)
	}
	return nil
}

type MySQLTablePositionState struct {
	sync.Mutex `toml:"-" json:"-"`
	Start      *utils.MySQLBinlogPosition    `toml:"start-binlog" json:"start-binlog"`
	Min        map[string]MySQLTablePosition `toml:"min" json:"min"`
	Max        map[string]MySQLTablePosition `toml:"max" json:"max"`
	Current    map[string]MySQLTablePosition `toml:"current" json:"current"`
}

func (tablePositionState *MySQLTablePositionState) Get() interface{} {
	return tablePositionState
}

func (tablePositionState *MySQLTablePositionState) GetRaw() string {
	ret, err := tablePositionState.ToJSON()
	if err != nil {
		log.Fatalf("[MySQLTablePositionState.GetRaw] error ToJSON. %#v. err: %s", tablePositionState, err)
	}
	return ret
}

func (tablePositionState *MySQLTablePositionState) PutRaw(pos string) {
	if pos == "" {
		*tablePositionState = MySQLTablePositionState{}
	} else {
		if err := myJson.UnmarshalFromString(pos, tablePositionState); err != nil {
			log.Fatalf("[MySQLTablePositionState.PutRaw] error put %s. err: %s", pos, err)
		}
	}
}

func (tablePositionState *MySQLTablePositionState) Put(pos interface{}) {
	*tablePositionState = pos.(MySQLTablePositionState)
}

func (tablePositionState *MySQLTablePositionState) Stage() stages.InputStage {
	return stages.InputStageFull
}

func (tablePositionState *MySQLTablePositionState) ToJSON() (string, error) {
	tablePositionState.Lock()
	defer tablePositionState.Unlock()
	return myJson.MarshalToString(tablePositionState)
}

func (tablePositionState *MySQLTablePositionState) GetStartBinlogPos() (utils.MySQLBinlogPosition, bool) {
	tablePositionState.Lock()
	defer tablePositionState.Unlock()

	if tablePositionState.Start == nil {
		return utils.MySQLBinlogPosition{}, false
	}

	return *tablePositionState.Start, true
}

func (tablePositionState *MySQLTablePositionState) PutStartBinlogPos(p utils.MySQLBinlogPosition) {
	tablePositionState.Lock()
	defer tablePositionState.Unlock()

	tablePositionState.Start = &p
}

func (tablePositionState *MySQLTablePositionState) GetMaxMin(sourceName string) (MySQLTablePosition, MySQLTablePosition, bool) {
	tablePositionState.Lock()
	defer tablePositionState.Unlock()

	if tablePositionState.Min == nil || tablePositionState.Max == nil {
		return MySQLTablePosition{}, MySQLTablePosition{}, false
	}

	max, okMax := tablePositionState.Max[sourceName]

	min, okMin := tablePositionState.Min[sourceName]
	return max, min, okMax && okMin
}

func (tablePositionState *MySQLTablePositionState) PutMaxMin(sourceName string, max MySQLTablePosition, min MySQLTablePosition) {
	tablePositionState.Lock()
	defer tablePositionState.Unlock()

	if tablePositionState.Min == nil {
		tablePositionState.Min = make(map[string]MySQLTablePosition)
	}

	if tablePositionState.Max == nil {
		tablePositionState.Max = make(map[string]MySQLTablePosition)
	}
	tablePositionState.Max[sourceName] = max
	tablePositionState.Min[sourceName] = min
}

func (tablePositionState *MySQLTablePositionState) GetCurrent(sourceName string) (MySQLTablePosition, bool) {
	tablePositionState.Lock()
	defer tablePositionState.Unlock()

	if tablePositionState.Current == nil {
		return MySQLTablePosition{}, false
	}

	p, ok := tablePositionState.Current[sourceName]
	return p, ok
}

func (tablePositionState *MySQLTablePositionState) PutCurrent(sourceName string, pos MySQLTablePosition) {
	tablePositionState.Lock()
	defer tablePositionState.Unlock()

	// log.Infof("[LoopInBatch] PutCurrent: sourceName: %v %v", sourceName, pos)

	if tablePositionState.Current == nil {
		tablePositionState.Current = make(map[string]MySQLTablePosition)
	}

	tablePositionState.Current[sourceName] = pos
}

func SerializeMySQLBinlogPosition(pos gomysql.Position, gtidSet gomysql.MysqlGTIDSet) utils.MySQLBinlogPosition {
	p := utils.MySQLBinlogPosition{}
	p.BinLogFileName = pos.Name
	p.BinLogFilePos = pos.Pos
	p.BinlogGTID = gtidSet.String()
	return p
}

func DeserializeMySQLBinlogPosition(p utils.MySQLBinlogPosition) (gomysql.Position, gomysql.MysqlGTIDSet, error) {
	gtidSet, err := gomysql.ParseMysqlGTIDSet(p.BinlogGTID)
	if err != nil {
		return gomysql.Position{}, gomysql.MysqlGTIDSet{}, errors.Trace(err)
	}

	mysqlGTIDSet := *gtidSet.(*gomysql.MysqlGTIDSet)
	return gomysql.Position{Name: p.BinLogFileName, Pos: p.BinLogFilePos}, mysqlGTIDSet, nil
}

type PipelineGravityMySQLPosition struct {
	CurrentPosition *utils.MySQLBinlogPosition `json:"current_position"`
	StartPosition   *utils.MySQLBinlogPosition `json:"start_position"`
}

func (p *PipelineGravityMySQLPosition) String() string {
	return p.GetRaw()
}

func (p *PipelineGravityMySQLPosition) Get() interface{} {
	return *p
}

func (p *PipelineGravityMySQLPosition) GetRaw() string {
	s, _ := myJson.MarshalToString(p)
	return s
}

func (p *PipelineGravityMySQLPosition) Put(pos interface{}) {
	*p = pos.(PipelineGravityMySQLPosition)
}

func (p *PipelineGravityMySQLPosition) PutRaw(pos string) {
	if err := myJson.UnmarshalFromString(pos, p); err != nil {
		log.Fatalf("[PipelineGravityMySQLPosition.PutRaw] %s", err)
	}
}

func (p *PipelineGravityMySQLPosition) Stage() stages.InputStage {
	return stages.InputStageIncremental
}
