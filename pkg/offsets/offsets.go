package offsets

import (
	"time"

	log "github.com/sirupsen/logrus"
)

// OffsetCommitRequest contains configuration options for requesting commit offset.
type OffsetCommitRequest struct {
	ConsumerGroup           string
	ConsumerGroupGeneration int32  // nothing to do
	ConsumerID              string // nothing to do
	RetentionTime           int64  // nothing to do

	blocks map[string]map[int32]*OffsetCommitRequestBlock
}

// AddBlock to set commit offset of request block
func (r *OffsetCommitRequest) AddBlock(topic string, partitionID int32, offset int64, timestamp int64, metadata string) {
	if r.blocks == nil {
		r.blocks = make(map[string]map[int32]*OffsetCommitRequestBlock)
	}

	if r.blocks[topic] == nil {
		r.blocks[topic] = make(map[int32]*OffsetCommitRequestBlock)
	}

	r.blocks[topic][partitionID] = &OffsetCommitRequestBlock{offset, timestamp, metadata}
}

func (r *OffsetCommitRequest) Blocks() map[string]map[int32]*OffsetCommitRequestBlock {
	return r.blocks
}

type OffsetCommitRequestBlock struct {
	Offset    int64
	Timestamp int64
	Metadata  string
}

// OffsetCommitResponse is the value for response of commit offset
type OffsetCommitResponse struct {
	Errors map[string]map[int32]error
}

// AddError to set err for response of commit offset
func (r *OffsetCommitResponse) AddError(topic string, partition int32, err error) {
	if r.Errors == nil {
		r.Errors = make(map[string]map[int32]error)
	}
	partitions := r.Errors[topic]
	if partitions == nil {
		partitions = make(map[int32]error)
		r.Errors[topic] = partitions
	}
	partitions[partition] = err
}

// OffsetFetchRequest contains configuration options for request of fetch offset
type OffsetFetchRequest struct {
	ConsumerGroup string
	partitions    map[string][]int32
}

// AddPartition to set partition for request
func (r *OffsetFetchRequest) AddPartition(topic string, partitionID int32) {
	if r.partitions == nil {
		r.partitions = make(map[string][]int32)
	}

	r.partitions[topic] = append(r.partitions[topic], partitionID)
}

func (r *OffsetFetchRequest) Partitions() map[string][]int32 {
	return r.partitions
}

// OffsetFetchResponseBlock contains configuration options for response of fetch offset block
type OffsetFetchResponseBlock struct {
	Offset   int64
	Metadata string
	Err      error
}

// OffsetFetchResponse contains configuration options for response of fetch offset
type OffsetFetchResponse struct {
	Blocks     map[string]map[int32]*OffsetFetchResponseBlock
	LastUpdate time.Time //only for db position store
}

// GetBlock to get fetch offset of response block
func (r *OffsetFetchResponse) GetBlock(topic string, partition int32) *OffsetFetchResponseBlock {
	if r.Blocks == nil {
		log.Warnf("[consumer] blocks == nil. topic %s, partition %d", topic, partition)
		return nil
	}

	if r.Blocks[topic] == nil {
		log.Warnf("[consumer] block is nil. blocks %+v, topic %s, partition %d", r.Blocks, topic, partition)
		return nil
	}

	return r.Blocks[topic][partition]
}

// AddBlock to set fetch offset of response block
func (r *OffsetFetchResponse) AddBlock(topic string, partitionID int32, offset int64, metadata string) {
	if r.Blocks == nil {
		r.Blocks = make(map[string]map[int32]*OffsetFetchResponseBlock)
	}

	partitions := r.Blocks[topic]
	if partitions == nil {
		partitions = make(map[int32]*OffsetFetchResponseBlock)
		r.Blocks[topic] = partitions
	}

	r.Blocks[topic][partitionID] = &OffsetFetchResponseBlock{
		Offset:   offset,
		Metadata: metadata,
		Err:      nil,
	}
}
