package batch_table_scheduler

import (
	"sync"

	"time"

	"github.com/juju/errors"
)

type workingElement struct {
	ack           chan struct{}
	nrWaitingItem int
}

type workingSet struct {
	sync.Mutex
	ticker   *time.Ticker
	elements map[string]*workingElement
}

func (ws *workingSet) checkConflict(k string) (ack chan struct{}, conflict bool) {
	ws.Lock()
	defer ws.Unlock()
	e, ok := ws.elements[k]
	if ok {
		if e.nrWaitingItem == 0 {
			return nil, false
		} else {
			return e.ack, true
		}

	} else {
		return nil, false
	}
}

func (ws *workingSet) checkConflictWithBatch(batch []string) ([]chan struct{}, bool) {
	ws.Lock()
	defer ws.Unlock()

	acks := make([]chan struct{}, len(batch))
	conflicts := make([]bool, len(batch))
	hasConflict := false

	for i, k := range batch {
		e, ok := ws.elements[k]
		if ok {
			conflicts[i] = true
			acks[i] = e.ack
			hasConflict = true
		} else {
			conflicts[i] = false
			acks[i] = nil
		}
	}

	return acks, hasConflict
}

func (ws *workingSet) put(k string) {
	ws.Lock()
	defer ws.Unlock()
	e, ok := ws.elements[k]
	if ok {
		e.nrWaitingItem++
	} else {
		ws.elements[k] = &workingElement{nrWaitingItem: 1, ack: make(chan struct{})}
	}
}

func (ws *workingSet) putBatch(batch []string) {
	ws.Lock()
	defer ws.Unlock()

	for _, k := range batch {
		e, ok := ws.elements[k]
		if ok {
			e.nrWaitingItem++
		} else {
			ws.elements[k] = &workingElement{nrWaitingItem: 1, ack: make(chan struct{})}
		}
	}
}

func (ws *workingSet) checkAndPut(k string) (hadConflict bool) {
	ack, conflict := ws.checkConflict(k)
	if conflict {
		<-ack
	}
	ws.put(k)
	return conflict
}

func (ws *workingSet) checkAndPutBatch(batch []string) (hadConflict bool) {
	acks, hadConflict := ws.checkConflictWithBatch(batch)
	if hadConflict {
		for _, ack := range acks {
			if ack != nil {
				<-ack
			}
		}
	}
	ws.putBatch(batch)
	return hadConflict
}

func (ws *workingSet) ack(k string) error {
	ws.Lock()
	defer ws.Unlock()

	element, ok := ws.elements[k]
	if !ok {
		return errors.Errorf("no element found in working set")
	}

	// one row's same update/insert may end up in the same working item key.
	element.nrWaitingItem--
	if element.nrWaitingItem == 0 {
		close(element.ack)
		delete(ws.elements, k)
	}
	return nil
}

func (ws *workingSet) numElements() int {
	ws.Lock()
	defer ws.Unlock()
	return len(ws.elements)
}

func newWorkingSet() *workingSet {
	ws := workingSet{elements: make(map[string]*workingElement), ticker: time.NewTicker(100 * time.Millisecond)}

	// we need a background goroutine here,=
	// since some component may not send ack back to scheduler
	go func() {
		for range ws.ticker.C {
			// big lock here
			ws.Lock()
			for k, e := range ws.elements {
				if e.nrWaitingItem == 0 {
					delete(ws.elements, k)
				}
			}
			ws.Unlock()
		}
	}()

	return &ws
}
