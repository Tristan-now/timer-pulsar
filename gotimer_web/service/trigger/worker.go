package trigger

import (
	"context"
	"fmt"
	"gotimer_web/common/conf"
	"gotimer_web/common/model/vo"
	"gotimer_web/common/utils"
	"gotimer_web/pkg/concurrency"
	"gotimer_web/pkg/log"
	"gotimer_web/pkg/pool"
	"gotimer_web/pkg/redis"
	"gotimer_web/service/executor"
	"strconv"
	"strings"
	"sync"
	"time"
)

type confProvider interface {
	Get() *conf.TriggerAppConf
}

type taskService interface {
	GetTasksByTime(ctx context.Context, key string, bucket int, start, end time.Time) ([]*vo.Task, error)
}

type Worker struct {
	task         taskService
	confProvider confProvider
	pool         pool.WorkerPool
	executor     *executor.Worker
	lockService  *redis.Client
}

func NewWorker(executor *executor.Worker, task *TaskService, lockService *redis.Client, confProvider *conf.TriggerAppConfProvider) *Worker {
	return &Worker{
		executor:     executor,
		task:         task,
		lockService:  lockService,
		pool:         pool.NewGoWorkerPool(confProvider.Get().WorkersNum),
		confProvider: confProvider,
	}
}

// 开启一个执行器的Start
func (w *Worker) Start(ctx context.Context) {
	w.executor.Start(ctx)
}

// 从bucket串中得到bucket的时间
func getStartMinute(slice string) (time.Time, error) {
	timeBucket := strings.Split(slice, "_")
	if len(timeBucket) != 2 {
		return time.Time{}, fmt.Errorf("invalid format of msg key: %s", slice)
	}
	return utils.GetStartMinute(timeBucket[0])
}

// 得到bucket的id
func getBucket(slice string) (int, error) {
	timeBucket := strings.Split(slice, "_")
	if len(timeBucket) != 2 {
		return -1, fmt.Errorf("invalid format of msg key : %s", slice)
	}
	return strconv.Atoi(timeBucket[1])
}

// 总的逻辑：根据key和起始时间得到task的vo视图，为每个task开启一个执行器
func (w *Worker) handleBatch(ctx context.Context, key string, start, end time.Time) error {
	//从输入的bucket编号得到bucket的id
	bucket, err := getBucket(key)
	if err != nil {
		return nil
	}
	// 寻找任务，先在 redis 根据key找，找不到再在 mysql 中通过bucket的id找
	tasks, err := w.task.GetTasksByTime(ctx, key, bucket, start, end)
	if err != nil {
		return err
	}
	timerIDs := make([]uint, 0, len(tasks))
	for _, task := range tasks {
		timerIDs = append(timerIDs, task.TimerID)
	}

	//遍历任务切片 ， 为每个任务开启一个执行器
	for _, task := range tasks {
		task := task
		if err := w.pool.Submit(func() {
			if err := w.executor.Work(ctx, utils.UnionTimerIDUnix(task.TimerID, task.RunTimer.UnixMilli())); err != nil {
				log.ErrorContextf(ctx, "executor work failed,err: %v", err)
			}
		}); err != nil {
			return err
		}
	}
	return nil
}

// 总逻辑：从制定起始时间执行一分钟，每次找出1s范围内的task去执行
func (w *Worker) Work(ctx context.Context, minuteBucketKey string, ack func()) error {
	startTime, err := getStartMinute(minuteBucketKey)
	if err != nil {
		return err
	}

	conf := w.confProvider.Get()
	// ZRangeGapSeconds = 1 ,1s轮询一次
	ticker := time.NewTicker(time.Duration(conf.ZRangeGapSeconds) * time.Second)
	defer ticker.Stop()

	//一次work中endTime为一分钟后
	endTime := startTime.Add(time.Minute)

	//ZRangeGapSeconds = 1s,NewSafeChan 创建的通道大小将是 61
	//通道可以存储 61 个消息，直到它被关闭或者消息被取出
	notifier := concurrency.NewSafeChan(int(time.Minute/(time.Duration(conf.ZRangeGapSeconds)*time.Second)) + 1)
	defer notifier.Close()

	var wg sync.WaitGroup
	wg.Add(1)
	//在 for range ticker.C 循环之前启动一个 goroutine 的原因是:
	//处理从 startTime 到 startTime 加上 ZRangeGapSeconds 秒之间的第一批数据。
	//这个 goroutine 被启动后，会立即开始执行，而不需要等待 Ticker 的第一个滴答信号
	go func() {
		defer wg.Done()
		if err := w.handleBatch(ctx, minuteBucketKey, startTime, startTime.Add(time.Duration(conf.ZRangeGapSeconds)*time.Second)); err != nil {
			notifier.Put(err)
		}
	}()

	for range ticker.C {
		select {
		case e := <-notifier.GetChan():
			err, _ := e.(error)
			return err
		default:
		}

		//startTime每次一秒累加
		//确保在处理时间间隔内的批次时，不会超出预定的结束时间endTime。
		//如果startTime已经达到或超过了endTime，这意味着所有应该处理的时间间隔都已经被处理完毕，
		//确保任务不会无限制地运行
		if startTime = startTime.Add(time.Duration(conf.ZRangeGapSeconds) * time.Second); startTime.Equal(endTime) || startTime.After(endTime) {
			break
		}

		wg.Add(1)
		go func(startTime time.Time) {
			defer wg.Done()
			if err := w.handleBatch(ctx, minuteBucketKey, startTime, startTime.Add(time.Duration(conf.ZRangeGapSeconds)*time.Second)); err != nil {
				notifier.Put(err)
			}

		}(startTime)
	}
	wg.Wait()
	select {
	case e := <-notifier.GetChan():
		err, _ = e.(error)
		return err
	default:
	}
	ack()
	log.InfoContextf(ctx, "ack success,key : %s", minuteBucketKey)
	return nil
}
