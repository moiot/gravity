package position_store

type PositionValueEncoder func(v interface{}) (string, error)
type PositionValueDecoder func(s string) (interface{}, error)

type PositionRepo interface {
	Get(pipelineName string) (Position, bool, error)
	Put(pipelineName string, position Position) error
	Delete(pipelineName string) error
	Close() error
}
