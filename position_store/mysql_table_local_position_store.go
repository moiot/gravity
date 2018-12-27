package position_store

import (
	"io/ioutil"
	"time"

	"github.com/juju/errors"
	log "github.com/sirupsen/logrus"
)

const DefaultMySQLTableLocalPositionFile = "mysql_table_position.json"

type mysqlTableLocalPositionStore struct {
	MySQLTablePositionState
	fileName string
}

func (store *mysqlTableLocalPositionStore) Start() error {
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		for {
			select {
			case <-ticker.C:
				if err := store.sync(); err != nil {
					log.Fatalf("failed to sync mysql table position: %v", errors.ErrorStack(err))
				}
			}
		}
	}()
	return nil
}

func (store *mysqlTableLocalPositionStore) Close() {
	log.Infof("[mysqlTableLocalPositionStore] closed")
}

func (store *mysqlTableLocalPositionStore) sync() error {
	buf, err := myJson.Marshal(&store.MySQLTablePositionState)
	if err != nil {
		return errors.Trace(err)
	}

	err = ioutil.WriteFile(store.fileName, buf, 0644)
	if err != nil {
		log.Errorf("[position_store]: Flush Position to file %s failed: %v", store.fileName, errors.ErrorStack(err))
		return errors.Trace(err)
	}
	return nil

}

func NewMySQLTableLocalPositionStore(fileName string) (MySQLTablePositionStore, error) {
	b, err := ioutil.ReadFile(fileName)
	if err != nil {
		return nil, errors.Trace(err)
	}

	state := MySQLTablePositionState{}
	if len(b) > 0 {
		err = myJson.Unmarshal(b, &state)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	store := mysqlTableLocalPositionStore{
		MySQLTablePositionState: state,
		fileName:                fileName,
	}

	return &store, nil
}
