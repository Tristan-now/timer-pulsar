package pool

import (
	"github.com/panjf2000/ants/v2"
	"time"
)

type WorkerPool interface {
	Submit(func()) error
}

type GoWorkerPool struct {
	pool *ants.Pool
}

func (g *GoWorkerPool) Submit(f func()) error { return g.pool.Submit(f) }

func NewGoWorkerPool(size int) *GoWorkerPool {
	pool, err := ants.NewPool(
		size,
		// 设置清理goroutine的间隙时间，此处设置一分钟清理一次
		ants.WithExpiryDuration(time.Minute),
	)
	if err != nil {
		panic(err)
	}
	return &GoWorkerPool{
		pool: pool,
	}
}
