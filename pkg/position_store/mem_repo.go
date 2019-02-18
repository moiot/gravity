package position_store

import "github.com/juju/errors"

type memPosition struct {
	meta PositionMeta
	v    string
}
type memRepo struct {
	positionRepoModels map[string]memPosition
}

func (repo *memRepo) Get(pipelineName string) (PositionMeta, string, bool, error) {
	p, ok := repo.positionRepoModels[pipelineName]
	if ok {
		if err := p.meta.Validate(); err != nil {
			return PositionMeta{}, "", true, errors.Trace(err)
		} else {
			return p.meta, p.v, true, nil
		}
	} else {
		return PositionMeta{}, "", false, nil
	}
}

func (repo *memRepo) Put(pipelineName string, meta PositionMeta, v string) error {
	meta.Name = pipelineName
	if err := meta.Validate(); err != nil {
		return errors.Trace(err)
	}

	repo.positionRepoModels[pipelineName] = memPosition{meta: meta, v: v}
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
	return &memRepo{positionRepoModels: make(map[string]memPosition)}
}
