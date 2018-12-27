package protocol_test

import (
	. "github.com/moiot/gravity/pkg/protocol"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

type jobMsg struct {
	KafkaMsgMeta
}

func (j *jobMsg) GetMessageRouter() MessageRouter {
	return &j.KafkaMsgMeta
}

var _ = Describe("KafkaMsgMeta test", func() {
	It("returns right partition", func() {
		expectedPartitions := []int32{1, 2}

		meta := KafkaMsgMeta{}
		meta.SetKafkaPartitions(expectedPartitions)
		Expect(meta.GetPartitions()).To(BeEquivalentTo(expectedPartitions))

		jobMsg := &jobMsg{}
		jobMsg.SetKafkaPartitions(expectedPartitions)
		Expect(jobMsg.GetPartitions()).To(BeEquivalentTo(expectedPartitions))
		Expect(jobMsg.GetMessageRouter().GetPartitions()).To(BeEquivalentTo(expectedPartitions))
	})
})
