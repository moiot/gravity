package tidb_kafka

import (
	"github.com/json-iterator/go"
	"github.com/moiot/gravity/pkg/position_store"

	"github.com/juju/errors"

	"github.com/moiot/gravity/pkg/sarama_cluster"

	"github.com/moiot/gravity/pkg/offsets"
)

var myJson = jsoniter.Config{SortMapKeys: true}.Froze()

type KafkaOffsetStoreFactory struct {
	pipelineName  string
	positionCache position_store.PositionCacheInterface
}

type TopicOffset map[int32]int64

type ConsumerGroupOffset map[string]TopicOffset

type KafkaPositionValue struct {
	Offsets map[string]ConsumerGroupOffset `json:"offsets"`
}

func Serialize(position *KafkaPositionValue) (string, error) {
	s, err := myJson.MarshalToString(position)
	if err != nil {
		return "", errors.Trace(err)
	}
	return s, nil
}

func Deserialize(value string) (*KafkaPositionValue, error) {
	position := KafkaPositionValue{}
	if err := myJson.UnmarshalFromString(value, &position); err != nil {
		return nil, errors.Trace(err)
	}
	return &position, nil
}

func (f *KafkaOffsetStoreFactory) GenOffsetStore(c *sarama_cluster.Consumer) sarama_cluster.OffsetStore {
	return &OffsetStore{positionCache: f.positionCache, pipelineName: f.pipelineName}
}

func NewKafkaOffsetStoreFactory(pipelineName string, positionCache position_store.PositionCacheInterface) *KafkaOffsetStoreFactory {
	return &KafkaOffsetStoreFactory{
		pipelineName:  pipelineName,
		positionCache: positionCache,
	}
}

type OffsetStore struct {
	pipelineName  string
	positionCache position_store.PositionCacheInterface
}

func (store *OffsetStore) CommitOffset(req *offsets.OffsetCommitRequest) (*offsets.OffsetCommitResponse, error) {
	position := store.positionCache.Get()

	positionValue, err := Deserialize(position.Value)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if _, ok := positionValue.Offsets[req.ConsumerGroup]; !ok {
		positionValue.Offsets[req.ConsumerGroup] = make(map[string]TopicOffset)
	}

	for topic, pbs := range req.Blocks() {
		for partition, b := range pbs {
			if _, ok := positionValue.Offsets[topic]; !ok {
				positionValue.Offsets[req.ConsumerGroup][topic] = make(map[int32]int64)
			}
			positionValue.Offsets[req.ConsumerGroup][topic][partition] = b.Offset
		}
	}

	v, err := Serialize(positionValue)
	if err != nil {
		return nil, errors.Trace(err)
	}

	position.Value = v
	store.positionCache.Put(position)

	resp := &offsets.OffsetCommitResponse{}
	for topic, pbs := range req.Blocks() {
		for partition := range pbs {
			resp.AddError(topic, partition, nil)
		}
	}
	return resp, nil
}

func (store *OffsetStore) FetchOffset(req *offsets.OffsetFetchRequest) (*offsets.OffsetFetchResponse, error) {
	position := store.positionCache.Get()

	kafkaPositionValue, err := Deserialize(position.Value)
	if err != nil {
		return nil, errors.Trace(err)
	}

	consumerGroupOffset, ok := kafkaPositionValue.Offsets[req.ConsumerGroup]
	if !ok {
		return nil, errors.Errorf("consumer group offset empty")
	}

	resp := &offsets.OffsetFetchResponse{}
	for topic, topicOffset := range consumerGroupOffset {
		for partition, offset := range topicOffset {
			resp.AddBlock(topic, partition, offset, "")
		}
	}
	resp.LastUpdate = position.UpdateTime
	return resp, nil
}

func (store *OffsetStore) Close() error {
	return nil // should not close with kafka consumer. output needs to commit offset
}
