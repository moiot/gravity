package position_store_test

import (
	"strings"
	"sync"

	"github.com/json-iterator/go"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	. "github.com/moiot/gravity/pkg/position_store"
)

var _ = Describe("encode and decode table position", func() {
	It("serialize/deserialize string", func() {
		positions := map[string]MySQLTablePosition{
			"table_a": {
				Value: "a_string",
			},
		}
		positionString, err := jsoniter.MarshalToString(positions)
		Expect(err).To(BeNil())
		Expect(strings.Contains(positionString, "a_string")).To(BeTrue())

		newPositions := make(map[string]MySQLTablePosition)
		err = jsoniter.UnmarshalFromString(positionString, &newPositions)
		Expect(err).To(BeNil())
		Expect(newPositions["table_a"].Type).To(Equal("string"))
		Expect(newPositions["table_a"].Value).To(BeEquivalentTo("a_string"))

	})

	It("serialize/deserialize int", func() {
		positions := map[string]MySQLTablePosition{
			"table_a": {
				Value: 123,
			},
		}
		positionString, err := jsoniter.MarshalToString(positions)
		Expect(err).To(BeNil())
		Expect(strings.Contains(positionString, "123")).To(BeTrue())
		newPositions := make(map[string]MySQLTablePosition)
		err = jsoniter.UnmarshalFromString(positionString, &newPositions)
		Expect(err).To(BeNil())
		Expect(newPositions["table_a"].Type).To(Equal("int"))
		Expect(newPositions["table_a"].Value).To(BeEquivalentTo(123))
	})
})

var _ = Describe("concurrent access to PositionState", func() {
	It("should work without data race", func() {
		var wg sync.WaitGroup

		state := MySQLTablePositionState{}

		// 100 goroutine set current
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				state.PutCurrent("a", MySQLTablePosition{Type: "string", Value: 123, Column: "user_id"})
			}()
		}

		// 100 goroutine get current
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				state.GetCurrent("a")
			}()
		}

		// 100 goroutine save position
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				state.ToJSON()
			}()
		}

		wg.Wait()
	})
})
