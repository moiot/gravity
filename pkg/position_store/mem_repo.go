package position_store

import "github.com/juju/errors"

type memRepo struct {
	positionRepoModels map[string]Position
}

func (repo *memRepo) Get(pipelineName string) (Position, bool, error) {
	p, ok := repo.positionRepoModels[pipelineName]
	if ok {
		if err := p.ValidateWithValueString(); err != nil {
			return Position{}, true, errors.Trace(err)
		} else {
			return p, true, nil
		}
	} else {
		return Position{}, false, nil
	}
}

func (repo *memRepo) Put(pipelineName string, p Position) error {
	if err := p.ValidateWithValueString(); err != nil {
		return errors.Trace(err)
	}

	repo.positionRepoModels[pipelineName] = p
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
	return &memRepo{positionRepoModels: make(map[string]Position)}
}
