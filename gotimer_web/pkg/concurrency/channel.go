package concurrency

import (
	"context"
	"sync"
)

// 避免资源泄露和潜在的goroutine泄露
type SafeChan struct {
	sync.Once
	ctx   context.Context
	close func()
	ch    chan interface{}
}

func NewSafeChan(size int) *SafeChan {
	s := SafeChan{
		ch: make(chan interface{}, size),
	}
	s.ctx, s.close = context.WithCancel(context.Background())
	return &s
}

func (s *SafeChan) Put(element interface{}) {
	select {
	case <-s.ctx.Done():
	case s.ch <- element:
	default:
	}
}

func (s *SafeChan) GetChan() chan interface{} { return s.ch }

func (s *SafeChan) Get() interface{} { return <-s.ch }

func (s *SafeChan) Close() {
	s.Do(func() {
		s.close()
		close(s.ch)
	})
}
