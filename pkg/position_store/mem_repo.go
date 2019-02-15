package position_store

type memRepo struct {
	positionRepoModels map[string]*PositionWithValueString
}

func (repo *memRepo) Get(pipelineName string) (*PositionWithValueString, bool, error) {
	p, ok := repo.positionRepoModels[pipelineName]
	return p, ok, nil
}

func (repo *memRepo) Put(pipelineName string, m *PositionWithValueString) error {
	repo.positionRepoModels[pipelineName] = m
	return nil
}

func (repo *memRepo) Delete(pipelineName string) error {
	delete(repo.positionRepoModels, pipelineName)
	return nil
}

func (repo *memRepo) Close() error {
	return nil
}

func NewMemoRepo() PositionRepo {
	return &memRepo{positionRepoModels: make(map[string]*PositionWithValueString)}
}
