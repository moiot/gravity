package core

import (
	"github.com/moiot/gravity/pkg/config"
	"github.com/moiot/gravity/pkg/position_store"
)

type Input interface {
	NewPositionCache() (position_store.PositionCacheInterface, error)
	Start(emitter Emitter, positionCache position_store.PositionCacheInterface) error
	Close()
	Stage() config.InputMode
	Done(positionCache position_store.PositionCacheInterface) chan position_store.Position
	SendDeadSignal() error // for test only
	Wait()
	Identity() uint32
}
