package checker

type Result interface {
	needAlert() bool
}

type Diff struct {
	sourceSeg *segment
	targetSeg *segment
}

func (Diff) needAlert() bool {
	return true
}

type Same struct {
	sourceSeg *segment
	targetSeg *segment
}

func (Same) needAlert() bool {
	return false
}

type Timeout struct {
	sourceSeg *segment
	targetTag string
}

func (Timeout) needAlert() bool {
	return true
}

// Ready is just for test
type Ready struct {
}

func (Ready) needAlert() bool {
	return false
}
