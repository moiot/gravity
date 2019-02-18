package position_store

type PositionValueEncoder func(v interface{}) (string, error)
type PositionValueDecoder func(s string) (interface{}, error)

type PositionRepo interface {
	Get(pipelineName string) (PositionMeta, string, bool, error)
	Put(pipelineName string, positionMeta PositionMeta, v string) error
	Delete(pipelineName string) error
	Close() error
}
