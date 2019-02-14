package position_store

type memRepo struct {
	positions    map[string]Position
	valueEncoder PositionValueEncoder
	valueDecoder PositionValueDecoder
}

func (repo *memRepo) Get(pipelineName string) (Position, bool, error) {
	p, ok := repo.positions[pipelineName]
	return p, ok, nil
}

func (repo *memRepo) GetWithRawValue(pipelineName string) (Position, bool, error) {
	p, ok := repo.positions[pipelineName]
	return p, ok, nil
}

func (repo *memRepo) Put(pipelineName string, position Position) error {
	repo.positions[pipelineName] = position
	return nil
}

func (repo *memRepo) Delete(pipelineName string) error {
	delete(repo.positions, pipelineName)
	return nil
}

func (repo *memRepo) Close() error {
	return nil
}

func (repo *memRepo) SetEncoderDecoder(encoder PositionValueEncoder, decoder PositionValueDecoder) {
	repo.valueEncoder = encoder
	repo.valueDecoder = decoder
}

func NewMemoRepo() PositionRepo {
	return &memRepo{positions: make(map[string]Position)}
}
