package core

import "github.com/moiot/gravity/pkg/position_cache"

type PositionCacheCreator interface {
	NewPositionCache() (position_cache.PositionCacheInterface, error)
}
