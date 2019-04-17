package core

import (
	"github.com/moiot/gravity/pkg/config"
	"github.com/moiot/gravity/pkg/position_cache"
	"github.com/moiot/gravity/pkg/position_repos"
)

type Input interface {
	Start(emitter Emitter, router Router, positionCache position_cache.PositionCacheInterface) error
	Close()
	Stage() config.InputMode
	Done() chan position_repos.Position
	SendDeadSignal() error // for test only
	Wait()
}
