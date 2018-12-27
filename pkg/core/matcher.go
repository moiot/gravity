package core

type IMatcher interface {
	Configure(arg interface{}) error
	Match(msg *Msg) bool
}

type IMatcherFactory interface {
	NewMatcher() IMatcher
}

type IMatcherGroup []IMatcher

// Match returns true if all matcher returns true
func (matcherGroup IMatcherGroup) Match(msg *Msg) bool {
	for _, m := range matcherGroup {
		if !m.Match(msg) {
			return false
		}
	}
	return true
}
