package position_store

import (
	"time"

	"github.com/moiot/gravity/gravity/inputs/stages"
)

type Position struct {
	Name       string
	Stage      stages.InputStage
	Raw        interface{}
	UpdateTime time.Time
}

type PositionStore interface {
	Start() error
	Close()
	Stage() stages.InputStage
	Position() Position
	Update(pos Position)
	Clear()
}
