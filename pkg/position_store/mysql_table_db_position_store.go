package position_store

import (
	"github.com/juju/errors"
	"github.com/moiot/gravity/pkg/utils"
	log "github.com/sirupsen/logrus"
)

type mysqlTableDBPositionStore struct {
	*dbPositionStore
}

func (s *mysqlTableDBPositionStore) position() *MySQLTablePositionState {
	return s.dbPositionStore.position.(*MySQLTablePositionState)
}

func (s *mysqlTableDBPositionStore) GetStartBinlogPos() (utils.MySQLBinlogPosition, bool) {
	return s.position().GetStartBinlogPos()
}

func (s *mysqlTableDBPositionStore) PutStartBinlogPos(position utils.MySQLBinlogPosition) {
	s.position().PutStartBinlogPos(position)
}

func (s *mysqlTableDBPositionStore) GetMaxMin(sourceName string) (max MySQLTablePosition, min MySQLTablePosition, ok bool) {
	log.Infof("[mysqlTableDBPositionStore] GetMaxMin")
	return s.position().GetMaxMin(sourceName)
}

func (s *mysqlTableDBPositionStore) PutMaxMin(sourceName string, max MySQLTablePosition, min MySQLTablePosition) {
	s.position().PutMaxMin(sourceName, max, min)
}

func (s *mysqlTableDBPositionStore) GetCurrent(sourceName string) (MySQLTablePosition, bool) {
	return s.position().GetCurrent(sourceName)
}

func (s *mysqlTableDBPositionStore) PutCurrent(sourceName string, pos MySQLTablePosition) {
	s.position().PutCurrent(sourceName, pos)
}

func NewMySQLTableDBPositionStore(name string, dbConfig *utils.DBConfig, annotation string) (*mysqlTableDBPositionStore, error) {
	db, err := utils.CreateDBConnection(dbConfig)
	if err != nil {
		return nil, errors.Trace(err)
	}
	wrapper, err := newDBPositionStore(name, db, annotation, &MySQLTablePositionState{})
	if err != nil {
		return nil, err
	}

	return &mysqlTableDBPositionStore{dbPositionStore: wrapper}, nil
}
