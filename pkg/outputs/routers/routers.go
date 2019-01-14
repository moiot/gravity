package routers

import (
	"github.com/moiot/gravity/pkg/core"
)

type RouteMatchers struct {
	AllMatchers []core.IMatcher
}

func (r RouteMatchers) Match(msg *core.Msg) bool {
	matched := true

	for _, m := range r.AllMatchers {
		if !m.Match(msg) {
			matched = false
			break
		}
	}
	return matched
}
