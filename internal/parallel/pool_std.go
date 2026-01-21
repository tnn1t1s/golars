package parallel

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
	p.tokens <- struct{}{}
	go func() {
		defer func() { <-p.tokens }()
		task()
	}()
	return nil
}

func (p *stdPool) Release() {}
