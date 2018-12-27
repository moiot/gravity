package checker

import (
	"crypto/md5"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/moiot/gravity/pkg/protocol/dcp"
)

var _ = Describe("Checker", func() {
	var c *checker
	srcTag := "src"
	targetTag := "target"

	BeforeEach(func() {
		c = New(&Config{srcTag, []string{targetTag}, 1})
	})

	AfterEach(func() {
		c.Shutdown()
	})

	It("should return Same for one identical batch", func() {
		payload := dcp.Payload{
			Id:      "123",
			Content: "content",
		}

		srcMsg := []dcp.Message{
			{
				Id:   "src-1",
				Tag:  srcTag,
				Body: &dcp.Message_Barrier{Barrier: 0},
			},
			{
				Id:  "src-2",
				Tag: srcTag,
				Body: &dcp.Message_Payload{
					Payload: &payload,
				},
				Timestamp: uint64(time.Now().Unix()),
				Checksum:  fmt.Sprintf("%x", md5.Sum([]byte(payload.Content))),
			},
			{
				Id:   "src-3",
				Tag:  srcTag,
				Body: &dcp.Message_Barrier{Barrier: 1},
			},
		}

		targetMsg := []dcp.Message{
			{
				Id:   "target-1",
				Tag:  targetTag,
				Body: &dcp.Message_Barrier{Barrier: 0},
			},
			{
				Id:  "target-2",
				Tag: targetTag,
				Body: &dcp.Message_Payload{
					Payload: &payload,
				},
				Timestamp: uint64(time.Now().Unix()),
				Checksum:  fmt.Sprintf("%x", md5.Sum([]byte(payload.Content))),
			},
			{
				Id:   "target-3",
				Tag:  targetTag,
				Body: &dcp.Message_Barrier{Barrier: 1},
			},
		}

		sendMsg(c, srcMsg, targetMsg)

		Expect(<-c.ResultChan).Should(Equal(&Same{
			sourceSeg: newSegment(srcMsg[1].Tag, srcMsg[0].GetBarrier(), srcMsg[2].GetBarrier(), []*dcp.Message{&(srcMsg[1])}),
			targetSeg: newSegment(targetMsg[1].Tag, targetMsg[0].GetBarrier(), targetMsg[2].GetBarrier(), []*dcp.Message{&(targetMsg[1])}),
		}))
	})

	It("should return Same for several batch with multiple update per payload id", func() {
		srcMsg := []dcp.Message{
			{
				Id:   "src-1",
				Tag:  srcTag,
				Body: &dcp.Message_Barrier{Barrier: 0},
			},
			{
				Id:  "src-2",
				Tag: srcTag,
				Body: &dcp.Message_Payload{
					Payload: &dcp.Payload{
						Id:      "1",
						Content: "1",
					},
				},
				Timestamp: 1,
				Checksum:  fmt.Sprintf("%x", md5.Sum([]byte("1"))),
			},
			{
				Id:  "src-3",
				Tag: srcTag,
				Body: &dcp.Message_Payload{
					Payload: &dcp.Payload{
						Id:      "1",
						Content: "2",
					},
				},
				Timestamp: 2,
				Checksum:  fmt.Sprintf("%x", md5.Sum([]byte("2"))),
			},
			{
				Id:  "src-4",
				Tag: srcTag,
				Body: &dcp.Message_Payload{
					Payload: &dcp.Payload{
						Id:      "2",
						Content: "1",
					},
				},
				Timestamp: 3,
				Checksum:  fmt.Sprintf("%x", md5.Sum([]byte("1"))),
			},
			{
				Id:  "src-5",
				Tag: srcTag,
				Body: &dcp.Message_Payload{
					Payload: &dcp.Payload{
						Id:      "1",
						Content: "3",
					},
				},
				Timestamp: 4,
				Checksum:  fmt.Sprintf("%x", md5.Sum([]byte("3"))),
			},
			{
				Id:   "src-6",
				Tag:  srcTag,
				Body: &dcp.Message_Barrier{Barrier: 1},
			},
			{
				Id:  "src-7",
				Tag: srcTag,
				Body: &dcp.Message_Payload{
					Payload: &dcp.Payload{
						Id:      "2",
						Content: "1",
					},
				},
				Timestamp: 7,
				Checksum:  fmt.Sprintf("%x", md5.Sum([]byte("1"))),
			},
			{
				Id:   "src-8",
				Tag:  srcTag,
				Body: &dcp.Message_Barrier{Barrier: 2},
			},
			{
				Id:  "src-9",
				Tag: srcTag,
				Body: &dcp.Message_Payload{
					Payload: &dcp.Payload{
						Id:      "2",
						Content: "1",
					},
				},
				Timestamp: 9,
				Checksum:  fmt.Sprintf("%x", md5.Sum([]byte("1"))),
			},
		}

		targetMsg := []dcp.Message{
			{
				Id:   "target-1",
				Tag:  targetTag,
				Body: &dcp.Message_Barrier{Barrier: 0},
			},
			{
				Id:  "target-2",
				Tag: targetTag,
				Body: &dcp.Message_Payload{
					Payload: &dcp.Payload{
						Id:      "2",
						Content: "1",
					},
				},
				Timestamp: 1,
				Checksum:  fmt.Sprintf("%x", md5.Sum([]byte("1"))),
			},
			{
				Id:  "target-3",
				Tag: targetTag,
				Body: &dcp.Message_Payload{
					Payload: &dcp.Payload{
						Id:      "1",
						Content: "1",
					},
				},
				Timestamp: 2,
				Checksum:  fmt.Sprintf("%x", md5.Sum([]byte("1"))),
			},
			{
				Id:  "target-4",
				Tag: targetTag,
				Body: &dcp.Message_Payload{
					Payload: &dcp.Payload{
						Id:      "1",
						Content: "2",
					},
				},
				Timestamp: 3,
				Checksum:  fmt.Sprintf("%x", md5.Sum([]byte("2"))),
			},
			{
				Id:  "target-5",
				Tag: targetTag,
				Body: &dcp.Message_Payload{
					Payload: &dcp.Payload{
						Id:      "1",
						Content: "3",
					},
				},
				Timestamp: 4,
				Checksum:  fmt.Sprintf("%x", md5.Sum([]byte("3"))),
			},
			{
				Id:   "target-6",
				Tag:  targetTag,
				Body: &dcp.Message_Barrier{Barrier: 1},
			},
			{
				Id:  "target-7",
				Tag: targetTag,
				Body: &dcp.Message_Payload{
					Payload: &dcp.Payload{
						Id:      "2",
						Content: "1",
					},
				},
				Timestamp: 7,
				Checksum:  fmt.Sprintf("%x", md5.Sum([]byte("1"))),
			},
			{
				Id:   "target-8",
				Tag:  targetTag,
				Body: &dcp.Message_Barrier{Barrier: 2},
			},
		}

		sendMsg(c, srcMsg, targetMsg)

		var result Result
		result = <-c.ResultChan
		Expect(result).Should(BeAssignableToTypeOf(&Same{}))
		Expect(result.(*Same).sourceSeg.startOffset).Should(Equal(uint64(0)))

		result = <-c.ResultChan
		Expect(result).Should(BeAssignableToTypeOf(&Same{}))
		Expect(result.(*Same).sourceSeg.startOffset).Should(Equal(uint64(1)))
	})

	It("should return Diff for different checksum", func() {
		payload := dcp.Payload{
			Id:      "123",
			Content: "content",
		}

		srcMsg := []dcp.Message{
			{
				Id:   "src-1",
				Tag:  srcTag,
				Body: &dcp.Message_Barrier{Barrier: 0},
			},
			{
				Id:  "src-2",
				Tag: srcTag,
				Body: &dcp.Message_Payload{
					Payload: &payload,
				},
				Timestamp: uint64(time.Now().Unix()),
				Checksum:  "1",
			},
			{
				Id:   "src-3",
				Tag:  srcTag,
				Body: &dcp.Message_Barrier{Barrier: 1},
			},
		}

		targetMsg := []dcp.Message{
			{
				Id:   "target-1",
				Tag:  targetTag,
				Body: &dcp.Message_Barrier{Barrier: 0},
			},
			{
				Id:  "target-2",
				Tag: targetTag,
				Body: &dcp.Message_Payload{
					Payload: &payload,
				},
				Timestamp: uint64(time.Now().Unix()),
				Checksum:  "2",
			},
			{
				Id:   "target-3",
				Tag:  targetTag,
				Body: &dcp.Message_Barrier{Barrier: 1},
			},
		}

		sendMsg(c, srcMsg, targetMsg)

		Expect(<-c.ResultChan).Should(BeAssignableToTypeOf(&Diff{}))
	})

	It("should return Timeout for uncompleted target message", func() {
		payload := dcp.Payload{
			Id:      "123",
			Content: "content",
		}

		srcMsg := []dcp.Message{
			{
				Id:   "src-1",
				Tag:  srcTag,
				Body: &dcp.Message_Barrier{Barrier: 0},
			},
			{
				Id:  "src-2",
				Tag: srcTag,
				Body: &dcp.Message_Payload{
					Payload: &payload,
				},
				Timestamp: uint64(time.Now().Unix()),
				Checksum:  "1",
			},
			{
				Id:   "src-3",
				Tag:  srcTag,
				Body: &dcp.Message_Barrier{Barrier: 1},
			},
		}

		targetMsg := []dcp.Message{
			{
				Id:   "target-1",
				Tag:  targetTag,
				Body: &dcp.Message_Barrier{Barrier: 0},
			},
			{
				Id:  "target-2",
				Tag: targetTag,
				Body: &dcp.Message_Payload{
					Payload: &payload,
				},
				Timestamp: uint64(time.Now().Unix()),
				Checksum:  "2",
			},
		}

		sendMsg(c, srcMsg, targetMsg)

		Expect(<-c.ResultChan).Should(BeAssignableToTypeOf(&Timeout{}))
	})
})

func sendMsg(c *checker, srcMsg []dcp.Message, targetMsg []dcp.Message) {
	go func() {
		for i := range srcMsg {
			c.Consume(&srcMsg[i])
		}
	}()

	go func() {
		for i := range targetMsg {
			c.Consume(&targetMsg[i])
		}
	}()
}
