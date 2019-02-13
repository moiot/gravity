package position_store

type PositionRepo interface {
	Get(pipelineName string) (Position, bool, error)
	Put(pipelineName string, position Position) error
	Delete(pipelineName string) error
	Close() error
}
