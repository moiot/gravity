package position_store

import (
	"time"

	"github.com/juju/errors"
	log "github.com/sirupsen/logrus"

	"github.com/moiot/gravity/pkg/config"
	"github.com/moiot/gravity/pkg/consts"
	"github.com/moiot/gravity/pkg/utils"
)

type mysqlBinlogDBPositionStore struct {
	*dbPositionStore

	startPositionInSpec *utils.MySQLBinlogPosition
}

func (s *mysqlBinlogDBPositionStore) Start() error {
	savedPosition := s.position.(*PipelineGravityMySQLPosition)

	// If the start position in the spec is different than the start position last saved,
	// we use the start position in the spec and override it.
	// otherwise, we just start normally
	log.Infof("[mysqlBinlogDBPositionStore] start position in spec %v, saved start position %v", s.startPositionInSpec.BinlogGTID, savedPosition.StartPosition.BinlogGTID)
	if s.startPositionInSpec.BinlogGTID != "" && !isPositionEquals(s.startPositionInSpec, savedPosition.StartPosition) {
		log.Infof("[mysqlBinlogDBPositionStore]: spec violation!")
		*savedPosition.StartPosition = *s.startPositionInSpec
		*savedPosition.CurrentPosition = *s.startPositionInSpec
	}

	// If there the start position and current position is empty we fetch the position from master
	if savedPosition.CurrentPosition.BinlogGTID == "" && savedPosition.StartPosition.BinlogGTID == "" {
		log.Infof("[mysqlBinlogDBPositionStore] load position from master")

		dbUtil := utils.NewMySQLDB(s.db)
		pos, gtidSet, err := dbUtil.GetMasterStatus()
		if err != nil {
			return errors.Trace(err)
		}
		position := utils.MySQLBinlogPosition{BinLogFileName: pos.Name, BinLogFilePos: pos.Pos, BinlogGTID: gtidSet.String()}
		*savedPosition.StartPosition = position
		*savedPosition.CurrentPosition = position
	}
	s.position = savedPosition

	log.Infof("[mysqlBinlogDBPositionStore] start with %s", s.position)

	return s.dbPositionStore.Start()
}

func (s *mysqlBinlogDBPositionStore) Get() utils.MySQLBinlogPosition {
	posPtr := s.dbPositionStore.Position().Raw.(PipelineGravityMySQLPosition).CurrentPosition
	if posPtr != nil {
		return *posPtr
	} else {
		return utils.MySQLBinlogPosition{}
	}
}

func (s *mysqlBinlogDBPositionStore) Put(position utils.MySQLBinlogPosition) {
	prev := s.dbPositionStore.Position().Raw.(PipelineGravityMySQLPosition)
	if prev.CurrentPosition == nil {
		prev.CurrentPosition = &utils.MySQLBinlogPosition{}
	}
	prev.CurrentPosition.BinLogFilePos = position.BinLogFilePos
	prev.CurrentPosition.BinLogFileName = position.BinLogFileName
	prev.CurrentPosition.BinlogGTID = position.BinlogGTID

	s.dbPositionStore.Update(Position{
		Name:       s.dbPositionStore.name,
		Stage:      config.Stream,
		Raw:        prev,
		UpdateTime: time.Now(),
	})
}

func (s *mysqlBinlogDBPositionStore) FSync() {
	s.dbPositionStore.savePosition()
}

func NewMySQLBinlogDBPositionStore(pipelineName string, dbConfig *utils.DBConfig, annotation string, startPositionInSpec *utils.MySQLBinlogPosition) (*mysqlBinlogDBPositionStore, error) {
	dbConn, err := utils.CreateDBConnection(dbConfig)
	if err != nil {
		return nil, errors.Trace(err)
	}

	wrapper, err := newDBPositionStore(pipelineName, dbConn, annotation, &PipelineGravityMySQLPosition{
		StartPosition:   &utils.MySQLBinlogPosition{},
		CurrentPosition: &utils.MySQLBinlogPosition{},
	})
	if err != nil {
		return nil, err
	}

	if startPositionInSpec == nil {
		startPositionInSpec = &utils.MySQLBinlogPosition{}
	}

	return &mysqlBinlogDBPositionStore{dbPositionStore: wrapper, startPositionInSpec: startPositionInSpec}, nil
}

func IsPositionStoreEvent(schema string, tableName string) bool {
	return schema == consts.GravityDBName && (tableName == positionTableName)
}
