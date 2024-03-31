package trigger

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"gotimer_trigger/common/conf"
	"gotimer_trigger/common/model/vo"
	"gotimer_trigger/common/utils"
	"gotimer_trigger/mq"
	"gotimer_trigger/pkg/concurrency"
	"gotimer_trigger/pkg/log"
	"gotimer_trigger/pkg/pool"
	"gotimer_trigger/pkg/redis"
)

type Worker struct {
	task         taskService
	confProvider confProvider
	pool         pool.WorkerPool
	lockService  *redis.Client
	pc           *mq.PulsarClient
	Consumer     pulsar.Consumer
	Producer     pulsar.Producer
}

func NewWorker(task *TaskService, lockService *redis.Client, confProvider *conf.TriggerAppConfProvider) *Worker {
	pc := mq.GetPulsarClient()
	consumer, err := pc.Client.Subscribe(pulsar.ConsumerOptions{
		Topic:            "scheduler-topic",
		SubscriptionName: "my-sub",
		Type:             pulsar.Shared,
	})

	producer, err := pc.Client.CreateProducer(pulsar.ProducerOptions{
		Topic: "trigger-topic",
	})

	if err != nil {
		log.Errorf("trigger consumer init failed,%v", err)
	}

	return &Worker{
		Producer:     producer,
		Consumer:     consumer,
		pc:           pc,
		task:         task,
		lockService:  lockService,
		pool:         pool.NewGoWorkerPool(confProvider.Get().WorkersNum),
		confProvider: confProvider,
	}
}

func (w *Worker) Work(ctx context.Context, minuteBucketKey string, ack func()) error {
	// log.InfoContextf(ctx, "trigger_1 start: %v", time.Now())
	// defer func() {
	// 	log.InfoContextf(ctx, "trigger_1 end: %v", time.Now())
	// }()

	// 进行为时一分钟的 zrange 处理
	startTime, err := getStartMinute(minuteBucketKey)
	if err != nil {
		return err
	}

	conf := w.confProvider.Get()
	ticker := time.NewTicker(time.Duration(conf.ZRangeGapSeconds) * time.Second)
	defer ticker.Stop()

	endTime := startTime.Add(time.Minute)

	notifier := concurrency.NewSafeChan(int(time.Minute/(time.Duration(conf.ZRangeGapSeconds)*time.Second)) + 1)
	defer notifier.Close()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		// log.InfoContextf(ctx, "trigger_2 start: %v", time.Now())
		// defer func() {
		// 	log.InfoContextf(ctx, "trigger_2 end: %v", time.Now())
		// }()
		defer wg.Done()
		if err := w.handleBatch(ctx, minuteBucketKey, startTime, startTime.Add(time.Duration(conf.ZRangeGapSeconds)*time.Second)); err != nil {
			notifier.Put(err)
		}
	}()
	for range ticker.C {
		//fmt.Println("trigger ticker", time.Now())
		select {
		case e := <-notifier.GetChan():
			err, _ = e.(error)
			return err
		default:
		}

		if startTime = startTime.Add(time.Duration(conf.ZRangeGapSeconds) * time.Second); startTime.Equal(endTime) || startTime.After(endTime) {
			break
		}

		// log.InfoContextf(ctx, "start time: %v", startTime)

		wg.Add(1)
		go func(startTime time.Time) {
			// log.InfoContextf(ctx, "trigger_2 start: %v", time.Now())
			// defer func() {
			// 	log.InfoContextf(ctx, "trigger_2 end: %v", time.Now())
			// }()
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
	log.InfoContextf(ctx, "ack success, key: %s", minuteBucketKey)
	return nil
}

func (w *Worker) handleBatch(ctx context.Context, key string, start, end time.Time) error {
	bucket, err := getBucket(key)
	if err != nil {
		return err
	}

	tasks, err := w.task.GetTasksByTime(ctx, key, bucket, start, end)
	//fmt.Println("在起始时间 ", start, " 截至时间 ", end, " ,得到task", tasks)
	// 对于两分钟一次的任务，只会在分钟起始第0s,解析到任务
	if err != nil {
		return err
	}

	timerIDs := make([]uint, 0, len(tasks))
	for _, task := range tasks {
		timerIDs = append(timerIDs, task.TimerID)
	}
	log.InfoContextf(ctx, "key: %s, get tasks: %+v, start: %v, end: %v", key, timerIDs, start, end)
	for _, task := range tasks {
		// issue_01:task重复声明
		//task := task
		fmt.Println("task :", task.TimerID, task.RunTimer)
		if err := w.pool.Submit(func() {
			log.InfoContext(context.Background(), "ants pool submit")
			// log.InfoContextf(ctx, "trigger_3 start: %v", time.Now())
			// defer func() {
			// 	log.InfoContextf(ctx, "trigger_3 end: %v", time.Now())
			// }()
			log.InfoContext(context.Background(), "task.TimerID = ", task.TimerID)
			log.InfoContext(context.Background(), "task.RunTimer.UnixMilli = ", task.RunTimer.UnixMilli())

			_, err := w.Producer.Send(ctx, &pulsar.ProducerMessage{
				Payload: []byte(utils.UnionTimerIDUnix(task.TimerID, task.RunTimer.UnixMilli())),
			},
			)
			if err != nil {
				log.Errorf("trigger msg send failed,%v", err)
			}
			log.InfoContext(context.Background(), "send msg : ", string([]byte(utils.UnionTimerIDUnix(task.TimerID, task.RunTimer.UnixMilli()))))
			//
			//if err := w.executor.Work(ctx, utils.UnionTimerIDUnix(task.TimerID, task.RunTimer.UnixMilli())); err != nil {
			//	log.ErrorContextf(ctx, "executor work failed, err: %v", err)
			//}

		}); err != nil {
			return err
		}
	}
	return nil
}

func getStartMinute(slice string) (time.Time, error) {
	timeBucket := strings.Split(slice, "_")
	if len(timeBucket) != 2 {
		return time.Time{}, fmt.Errorf("invalid format of msg key: %s", slice)
	}

	return utils.GetStartMinute(timeBucket[0])
}

func getBucket(slice string) (int, error) {
	timeBucket := strings.Split(slice, "_")
	if len(timeBucket) != 2 {
		return -1, fmt.Errorf("invalid format of msg key: %s", slice)
	}
	return strconv.Atoi(timeBucket[1])
}

type taskService interface {
	GetTasksByTime(ctx context.Context, key string, bucket int, start, end time.Time) ([]*vo.Task, error)
}

type confProvider interface {
	Get() *conf.TriggerAppConf
}
