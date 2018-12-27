package batch_table_scheduler

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"math/rand"

	"github.com/stretchr/testify/assert"
)

func TestWorkingSet(t *testing.T) {
	assert := assert.New(t)

	t.Run("simple element put and ack", func(tt *testing.T) {
		ws := newWorkingSet()
		k := "1"
		// ack a non exist key will return error
		assert.NotNil(ws.ack("fake"))

		// put a new key does not block
		b := ws.checkAndPut(k)
		assert.False(b)

		_, b2 := ws.checkConflict(k)
		assert.True(b2)

		assert.Nil(ws.ack(k))

		_, b3 := ws.checkConflict(k)
		assert.False(b3)

		time.Sleep(time.Second)

		assert.Equal(0, ws.numElements())
	})

	t.Run("batch put and ack with the same key", func(tt *testing.T) {

		ws := newWorkingSet()

		// 100 size bath with the same key
		batch := make([]string, 100)
		for i := 0; i < 100; i++ {
			batch[i] = fmt.Sprintf("%d", i)
		}

		hadBlock := ws.checkAndPutBatch(batch)
		assert.False(hadBlock)

		// put batch again
		ack, hadBlock := ws.checkConflictWithBatch(batch)
		assert.True(hadBlock)

		// not start a goroutine and wait for ack
		var wg sync.WaitGroup
		wg.Add(1)

		go func() {
			defer wg.Done()
			for _, c := range ack {
				<-c
			}
		}()

		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func(batchIdx int) {
				defer wg.Done()
				ws.ack(batch[batchIdx])
			}(i)
		}
		wg.Wait()

		// not we are good to go with the key
		_, hadBlock = ws.checkConflictWithBatch(batch)
		assert.False(hadBlock)

	})

	var sequenceKeys = func(num int) []string {
		ret := make([]string, num)
		for i := 0; i < num; i++ {
			ret[i] = fmt.Sprintf("%d", i)
		}
		return ret
	}

	var randomKeys = func(num int) []string {
		ret := make([]string, num)
		for i := 0; i < num; i++ {
			ret[i] = fmt.Sprintf("%d", rand.Int()%num)
		}
		return ret
	}

	var theSameKeys = func(num int) []string {
		k := rand.Int() % num
		ret := make([]string, num)
		for i := 0; i < num; i++ {
			ret[i] = fmt.Sprintf("%d", k)
		}
		return ret
	}

	t.Run("run a serials of step that may conflict", func(tt *testing.T) {
		// 100 batches
		var batches [][]string
		for i := 0; i < 100; i++ {
			b := [][]string{
				sequenceKeys(100),
				randomKeys(100),
				theSameKeys(100),
			}

			batches = append(batches, b...)
		}

		batchC := make(chan []string)

		ws := newWorkingSet()

		var wg sync.WaitGroup

		// these goroutine adds stuff to working set
		for _, batch := range batches {
			wg.Add(1)

			go func(b []string) {
				defer wg.Done()
				acks, _ := ws.checkConflictWithBatch(b)
				for _, ack := range acks {
					<-ack
					batchC <- b
				}
			}(batch)
		}

		// these goroutines ack stuff in working set
		for range batches {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for range batchC {
				}
			}()
		}

		wg.Done()

		// now we are clean
		for _, batch := range batches {
			_, hadConflict := ws.checkConflictWithBatch(batch)
			assert.False(hadConflict)
		}
	})

	t.Run("large number of random put and ack", func(tt *testing.T) {
		ws := newWorkingSet()
		nrKeys := 1000
		keys := make([]string, nrKeys)

		// setup the working set
		for i := 0; i < nrKeys; i++ {
			keys[i] = fmt.Sprintf("%d", i)
			ws.checkAndPut(keys[i])
		}

		// try to put, ack in parallel
		var wg sync.WaitGroup
		for i := 0; i < nrKeys; i++ {
			wg.Add(2)

			go func(k string) {
				defer wg.Done()
				ws.checkAndPut(k)
			}(keys[i])

			go func(k string) {
				defer wg.Done()
				ws.ack(k)
			}(keys[i])
		}
		wg.Wait()
		// eventually, the working set should be full again
		assert.Equal(nrKeys, ws.numElements())
	})
}
