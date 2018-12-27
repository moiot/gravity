package core

import (
	"github.com/moiot/gravity/gravity/inputs/position_store"
	"github.com/moiot/gravity/gravity/inputs/stages"
)

type Input interface {
	Start(emitter Emitter) error
	Close()
	Stage() stages.InputStage
	// TODO position store can be hidden by input plugin
	// or we should use a configuration dedicated for position store
	NewPositionStore() (position_store.PositionStore, error)
	PositionStore() position_store.PositionStore
	Done() chan position_store.Position
	SendDeadSignal() error // for test only
	Wait()
	Identity() uint32
}
