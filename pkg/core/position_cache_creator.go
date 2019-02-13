package core

import "github.com/moiot/gravity/pkg/position_store"

type PositionCacheCreator interface {
	NewPositionCache() (position_store.PositionCacheInterface, error)
}
