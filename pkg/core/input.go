package core

import (
	"github.com/moiot/gravity/pkg/config"
	"github.com/moiot/gravity/pkg/position_store"
)

type Input interface {
	Start(emitter Emitter) error
	Close()
	Stage() config.InputMode
	NewPositionCache() (*position_store.PositionCache, error)
	Done() chan position_store.Position
	SendDeadSignal() error // for test only
	Wait()
	Identity() uint32
}
