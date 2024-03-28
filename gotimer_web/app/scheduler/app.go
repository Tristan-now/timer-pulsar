package scheduler

import (
	"context"
	"gotimer_web/common/conf"
	"gotimer_web/pkg/log"
	service "gotimer_web/service/scheduler"
	"sync"
)

type workService interface {
	Start(context.Context) error
}

type confProvider interface {
	Get() *conf.SchedulerAppConf
}

type WorkerApp struct {
	sync.Once
	service workService
	ctx     context.Context
	stop    func()
}

func NewWorkerApp(service *service.Worker) *WorkerApp {
	w := WorkerApp{
		service: service,
	}
	w.ctx, w.stop = context.WithCancel(context.Background())
	return &w
}

func (w *WorkerApp) Start() {
	w.Do(w.start)
}

func (w *WorkerApp) start() {
	log.InfoContext(w.ctx, "worker app is starting")
	go func() {
		if err := w.service.Start(w.ctx); err != nil {
			log.ErrorContextf(w.ctx, "worker start failed,err: %v", err)
		}
	}()
}

func (w *WorkerApp) Stop() {
	w.stop()
	log.WarnContext(w.ctx, "worker app is stopped")
}
