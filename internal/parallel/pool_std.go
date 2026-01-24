package parallel

import "fmt"

type stdPool struct {
	tokens chan struct{}
}

func newPool(size int) (pooler, error) {
	if size < 1 {
		size = 1
	}
	return &stdPool{tokens: make(chan struct{}, size)}, nil
}

func (p *stdPool) Submit(task func()) error {
	select {
	case p.tokens <- struct{}{}:
		go func() {
			defer func() { <-p.tokens }()
			task()
		}()
		return nil
	default:
		return fmt.Errorf("parallel pool at capacity")
	}
}

func (p *stdPool) Release() {}
