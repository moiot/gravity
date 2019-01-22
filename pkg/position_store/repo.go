package position_store

type PositionRepo interface {
	Get(pipelineName string) (Position, error)
	Put(pipelineName string, position Position) error
}
