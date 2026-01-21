//go:build ants

package parallel

import "github.com/panjf2000/ants/v2"

type antsPool struct {
	pool *ants.Pool
}

func newPool(size int) (pooler, error) {
	pool, err := ants.NewPool(size)
	if err != nil {
		return nil, err
	}
	return &antsPool{pool: pool}, nil
}

func (p *antsPool) Submit(task func()) error {
	return p.pool.Submit(task)
}

func (p *antsPool) Release() {
	p.pool.Release()
}
