package sarama_cluster

import (
	"sync"

	"github.com/Shopify/sarama"
	"github.com/moiot/gravity/pkg/offsets"
)

// OffsetStash allows to accumulate offsets and
// mark them as processed in a bulk
type OffsetStash struct {
	offsets map[topicPartition]offsetInfo
	mu      sync.Mutex
}

// NewOffsetStash inits a blank stash
func NewOffsetStash() *OffsetStash {
	return &OffsetStash{offsets: make(map[topicPartition]offsetInfo)}
}

// MarkOffset stashes the provided message offset
func (s *OffsetStash) MarkOffset(msg *sarama.ConsumerMessage, metadata string) {
	s.MarkPartitionOffset(msg.Topic, msg.Partition, msg.Offset, metadata)
}

// MarkPartitionOffset stashes the offset for the provided topic/partition combination
func (s *OffsetStash) MarkPartitionOffset(topic string, partition int32, offset int64, metadata string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := topicPartition{Topic: topic, Partition: partition}
	if info := s.offsets[key]; offset >= info.Offset {
		info.Offset = offset
		info.Metadata = metadata
		s.offsets[key] = info
	}
}

// Offsets returns the latest stashed offsets by topic-partition
func (s *OffsetStash) Offsets() map[string]int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	res := make(map[string]int64, len(s.offsets))
	for tp, info := range s.offsets {
		res[tp.String()] = info.Offset
	}
	return res
}

type OffsetStoreFactory interface {
	GenOffsetStore(c *Consumer) OffsetStore
}

type DefaultOffsetStoreFactory struct {
}

func (f *DefaultOffsetStoreFactory) GenOffsetStore(c *Consumer) OffsetStore {
	return &BrokerOffsetStore{c: c}
}

type OffsetStore interface {
	CommitOffset(req *offsets.OffsetCommitRequest) (*offsets.OffsetCommitResponse, error)
	FetchOffset(req *offsets.OffsetFetchRequest) (*offsets.OffsetFetchResponse, error)
	Close() error
}

type BrokerOffsetStore struct {
	c *Consumer
}

func (manager *BrokerOffsetStore) CommitOffset(req *offsets.OffsetCommitRequest) (*offsets.OffsetCommitResponse, error) {
	broker, err := manager.c.client.Coordinator(manager.c.groupID)
	if err != nil {
		manager.c.closeCoordinator(broker, err)
		return nil, err
	}
	saramaReq := &sarama.OffsetCommitRequest{
		ConsumerGroup:           req.ConsumerGroup,
		ConsumerGroupGeneration: req.ConsumerGroupGeneration,
		ConsumerID:              req.ConsumerID,
		RetentionTime:           req.RetentionTime,
		Version:                 2,
	}
	for topic, bs := range req.Blocks() {
		for partition, b := range bs {
			saramaReq.AddBlock(topic, partition, b.Offset, b.Timestamp, b.Metadata)
		}
	}
	saramaResp, err := broker.CommitOffset(saramaReq)
	if err != nil {
		manager.c.closeCoordinator(broker, err)
		return nil, err
	}
	resp := &offsets.OffsetCommitResponse{}
	for topic, pe := range saramaResp.Errors {
		for partition, e := range pe {
			if e != sarama.ErrNoError {
				resp.AddError(topic, partition, e)
			}
		}
	}

	return resp, nil
}

func (manager *BrokerOffsetStore) FetchOffset(req *offsets.OffsetFetchRequest) (*offsets.OffsetFetchResponse, error) {
	c := manager.c
	broker, err := c.client.Coordinator(c.groupID)
	if err != nil {
		c.closeCoordinator(broker, err)
		return nil, err
	}

	saramaReq := &sarama.OffsetFetchRequest{
		ConsumerGroup: c.groupID,
	}
	for topic, ps := range req.Partitions() {
		for _, p := range ps {
			saramaReq.AddPartition(topic, p)
		}
	}
	saramaResp, err := broker.FetchOffset(saramaReq)
	if err != nil {
		c.closeCoordinator(broker, err)
		return nil, err
	}
	resp := &offsets.OffsetFetchResponse{}
	for topic, pbs := range saramaResp.Blocks {
		for partition, b := range pbs {
			resp.AddBlock(topic, partition, b.Offset, b.Metadata)
		}
	}

	return resp, nil
}

func (manager *BrokerOffsetStore) Close() error {
	return nil
}
