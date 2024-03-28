package main

import (
	"context"
	"github.com/spf13/viper"
	cf "gotimer_scheduler/common/conf"
	"gotimer_scheduler/pkg/log"
	"gotimer_scheduler/pkg/redis"
	"gotimer_scheduler/service/scheduler"
	service "gotimer_scheduler/service/scheduler"
	"os"
	"sync"
)

var defaultRedisConfProvider *cf.RedisConfigProvider
var defaultSchedulerAppConfProvider *cf.SchedulerAppConfProvider

// 兜底配置
var gConf GloablConf = GloablConf{
	Scheduler: &cf.SchedulerAppConf{
		// 单节点并行协程数
		WorkersNum: 100,
		// 分桶数量
		BucketsNum: 10,
		// 调度器获取分布式锁时初设的过期时间，单位：s
		TryLockSeconds: 70,
		// 调度器每次尝试获取分布式锁的时间间隔，单位：s
		TryLockGapMilliSeconds: 100,
		// 时间片执行成功后，更新的分布式锁时间，单位：s
		SuccessExpireSeconds: 130,
	},

	Redis: &cf.RedisConfig{
		Network: "tcp",
		// 最大空闲连接数
		MaxIdle: 2000,
		// 空闲连接超时时间，单位：s
		IdleTimeoutSeconds: 30,
		// 连接池最大存活的连接数
		MaxActive: 1000,
		// 当连接数达到上限时，新的请求是等待还是立即报错
		Wait: true,
	},
}

type GloablConf struct {
	Redis     *cf.RedisConfig      `yaml:"redis"`
	Scheduler *cf.SchedulerAppConf `yaml:"scheduler"`
}

// 读取配置启动多个协程进行
type WorkerApp struct {
	sync.Once
	service workerService
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

var wg sync.WaitGroup

func (w *WorkerApp) start() {
	log.InfoContext(w.ctx, "worker app is starting")
	wg.Add(1)
	go func() {
		if err := w.service.Start(w.ctx); err != nil {
			log.ErrorContextf(w.ctx, "worker start failed, err: %v", err)
		}
	}()
	wg.Wait()
}

func (w *WorkerApp) Stop() {
	w.stop()
	log.WarnContext(w.ctx, "worker app is stopped")
}

type workerService interface {
	Start(context.Context) error
}

func main() {
	// 获取项目的执行路径
	path, err := os.Getwd()
	if err != nil {
		panic(err)
	}

	config := viper.New()
	config.AddConfigPath(path)   //设置读取的文件路径
	config.SetConfigName("conf") //设置读取的文件名
	config.SetConfigType("yaml") //设置文件的类型
	// 尝试进行配置读取
	if err := config.ReadInConfig(); err != nil {
		panic(err)
	}

	if err := config.Unmarshal(&gConf); err != nil {
		panic(err)
	}
	defaultRedisConfProvider = cf.NewRedisConfigProvider(gConf.Redis)
	defaultSchedulerAppConfProvider = cf.NewSchedulerAppConfProvider(gConf.Scheduler)

	redisClient := redis.GetClient(defaultRedisConfProvider)
	Scheduler := scheduler.NewWorker(redisClient, defaultSchedulerAppConfProvider)
	schedulerApp := NewWorkerApp(Scheduler)
	schedulerApp.Start()
}
