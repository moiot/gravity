package position_store

type memRepo struct {
	positionRepoModels map[string]*PositionRepoModel
}

func (repo *memRepo) Get(pipelineName string) (*PositionRepoModel, bool, error) {
	p, ok := repo.positionRepoModels[pipelineName]
	return p, ok, nil
}

func (repo *memRepo) Put(pipelineName string, m *PositionRepoModel) error {
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
	return &memRepo{positionRepoModels: make(map[string]*PositionRepoModel)}
}
