package core

import (
	"github.com/moiot/gravity/pkg/config"
	"github.com/moiot/gravity/pkg/position_store"
)

type Input interface {
	Start(emitter Emitter, router Router, positionCache position_store.PositionCacheInterface) error
	Close()
	Stage() config.InputMode
	Done() chan position_store.Position
	SendDeadSignal() error // for test only
	Wait()
}
