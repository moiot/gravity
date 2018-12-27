package position_store

import (
	"database/sql"
	"fmt"
	"sync"
	"time"

	ipositionStore "github.com/moiot/gravity/gravity/inputs/position_store"
	"github.com/moiot/gravity/gravity/inputs/stages"

	"github.com/juju/errors"
	log "github.com/sirupsen/logrus"

	"github.com/moiot/gravity/pkg/utils/retry"
)

type dbPositionStore struct {
	position ISerializablePosition

	db         *sql.DB
	annotation string

	name       string
	stage      stages.InputStage
	updateTime time.Time

	sync.RWMutex

	wg        sync.WaitGroup
	closeC    chan struct{}
	closeOnce sync.Once
}

func (s *dbPositionStore) Stage() stages.InputStage {
	return s.stage
}

func (s *dbPositionStore) Start() error {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()

		ticker := time.NewTicker(time.Second * 5)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				s.savePosition()

			case <-s.closeC:
				s.savePosition()
				log.Info("[dbPositionStore] exit loop")
				return
			}
		}
	}()

	return nil
}

func (s *dbPositionStore) Close() {
	log.Info("[dbPositionStore.Close]...")

	s.closeOnce.Do(func() {
		close(s.closeC)
		s.wg.Wait()
	})

	log.Info("[dbPositionStore.Close] closed")
}

func (s *dbPositionStore) savePosition() {
	s.Lock()
	defer s.Unlock()

	if s.stage != s.position.Stage() {
		return
	}

	posString := s.position.GetRaw()

	stmt := fmt.Sprintf("%sINSERT INTO %s(name, stage, position) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE stage = ?, position = ?", s.annotation, positionFullTableName)
	err := retry.Do(func() error {
		_, err := s.db.Exec(stmt, s.name, s.stage, posString, s.stage, posString)
		return err
	}, 3, retry.DefaultSleep)
	if err != nil {
		log.Fatalf("[dbPositionStore.savePosition] fails. err: %s", err)
	}

	s.updateTime = time.Now()
	log.Infof("[dbPositionStore.savePosition] succeed: %s, %s", s.stage, posString)
}

func (s *dbPositionStore) loadPosition() {
	err := retry.Do(func() error {
		row := s.db.QueryRow(fmt.Sprintf("%sSELECT position, stage, updated_at FROM %s WHERE name = ?", s.annotation, positionFullTableName), s.name)
		var posStr string
		var stage string
		var lastUpdate time.Time
		err := row.Scan(&posStr, &stage, &lastUpdate)
		if err == sql.ErrNoRows {
			log.Infof("[dbPositionStore.loadPosition] position for %s is empty", s.name)
			return nil
		} else if err != nil {
			log.Warnf("[dbPositionStore.loadPosition] error query db. %s", err)
			return err
		} else {
			s.stage = stages.InputStage(stage)
			s.updateTime = lastUpdate
			if s.stage == s.position.Stage() {
				s.position.PutRaw(posStr)
			}
			return nil
		}
	}, 3, retry.DefaultSleep)

	if err != nil {
		log.Fatalf("[dbPositionStore.loadPosition] error query db. %s", err)
	}
}

func (s *dbPositionStore) Clear() {
	_, err := s.db.Exec(fmt.Sprintf("%sDELETE from %s WHERE name = ?", s.annotation, positionFullTableName), s.name)
	if err != nil {
		log.Fatalf("[dbPositionStore.Clear] error %s", err)
	}
	log.Infof("[dbPositionStore.Clear] cleared %s", s.Position())
}

func (s *dbPositionStore) Position() ipositionStore.Position {
	s.RLock()
	defer s.RUnlock()

	return ipositionStore.Position{
		Name:       s.name,
		Stage:      s.stage,
		Raw:        s.position.Get(),
		UpdateTime: s.updateTime,
	}
}

func (s *dbPositionStore) Update(pos ipositionStore.Position) {
	s.Lock()
	defer s.Unlock()

	if s.position.Stage() != pos.Stage {
		log.Fatalf("[dbPositionStore.Update] expect stage %s, actual %#v", s.position.Stage(), pos)
	}
	s.name = pos.Name
	s.updateTime = pos.UpdateTime
	s.stage = pos.Stage
	s.position.Put(pos.Raw)
}

func newDBPositionStore(pipelineName string, metaRepoDB *sql.DB, annotation string, delegate ISerializablePosition) (*dbPositionStore, error) {
	// var metaRepoSource *utils.DBConfig
	// var annotation string
	// metaRepoCfg := pipelineConfig.SourceProbeCfg
	// if metaRepoCfg != nil {
	// 	if metaRepoCfg.SourceMySQL != nil {
	// 		metaRepoSource = metaRepoCfg.SourceMySQL
	// 	}
	// 	if metaRepoCfg.Annotation != "" {
	// 		annotation = fmt.Sprintf("/*%s*/", metaRepoCfg.Annotation)
	// 	}
	// } else {
	// 	metaRepoSource = pipelineConfig.MySQLConfig.Source
	// }
	// metaRepoDB, err := utils.CreateDBConnection(metaRepoSource)
	// if err != nil {
	// 	return nil, errors.Trace(err)
	// }

	store := &dbPositionStore{
		name:       pipelineName,
		db:         metaRepoDB,
		annotation: annotation,
		closeC:     make(chan struct{}),
		position:   delegate,
		stage:      delegate.Stage(),
	}

	if err := PrepareMetaRepo(metaRepoDB, annotation); err != nil {
		return nil, errors.Trace(err)
	}

	store.loadPosition()

	return store, nil
}
