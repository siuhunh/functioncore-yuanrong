package main

import "sync"

// Flyer -
type Flyer struct {
	concurrency int
	newLocal    func(id int)
	fn          func(id int, input interface{})
}

// NewFlyer -
func NewFlyer(
	concurrency int,
	newLocal func(id int),
	fn func(id int, input interface{})) *Flyer {

	return &Flyer{
		concurrency: concurrency,
		newLocal:    newLocal,
		fn:          fn,
	}
}

// Run -
func (f *Flyer) Run() (inputCh chan<- interface{}, wait func()) {
	ch := make(chan interface{})

	var wg sync.WaitGroup
	wg.Add(f.concurrency)
	for i := 0; i < f.concurrency; i++ {
		go func(id int) {
			defer wg.Done()

			f.newLocal(id)

			for {
				input, ok := <-ch
				if !ok {
					return
				}

				f.fn(id, input)
			}
		}(i)
	}

	return ch, func() { wg.Wait() }
}
