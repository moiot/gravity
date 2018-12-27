package sliding_window

const FirstSequenceNumber = 1

type SequenceGenerator struct {
	sequenceNumber int64
}

func (generator *SequenceGenerator) Next() int64 {
	generator.sequenceNumber++
	return generator.sequenceNumber
}

func NewSequenceGenerator() *SequenceGenerator {
	g := SequenceGenerator{sequenceNumber: FirstSequenceNumber - 1}
	return &g
}
