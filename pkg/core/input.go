package core

import (
	"github.com/moiot/gravity/pkg/config"
	"github.com/moiot/gravity/pkg/position_store"
)

type Input interface {
	Start(emitter Emitter) error
	Close()
	Stage() config.InputMode
	// TODO position store can be hidden by input plugin
	// or we should use a configuration dedicated for position store
	NewPositionStore() (position_store.PositionStore, error)
	PositionStore() position_store.PositionStore
	Done() chan position_store.Position
	SendDeadSignal() error // for test only
	Wait()
	Identity() uint32
}
