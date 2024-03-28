package scheduler

import (
	"context"
	"gotimer_web/common/conf"
	"gotimer_web/common/consts"
	"gotimer_web/common/utils"
	"gotimer_web/pkg/log"
	"gotimer_web/pkg/pool"
	"gotimer_web/pkg/redis"
	"gotimer_web/service/trigger"
	"strconv"
	"time"
)

type appConfProvider interface {
	Get() *conf.SchedulerAppConf
}

type lockService interface {
	GetDistributionLock(key string) redis.DistributeLocker
}

type bucketGetter interface {
	Get(ctx context.Context, key string) (string, error)
}

type Worker struct {
	pool            pool.WorkerPool
	appConfProvider appConfProvider
	trigger         *trigger.Worker
	lockService     lockService
	bucketGetter    bucketGetter
	minuteBuckets   map[string]int
}

func NewWorker(trigger *trigger.Worker, redisClient *redis.Client, appConfProvider *conf.SchedulerAppConfProvider) *Worker {
	return &Worker{
		pool:            pool.NewGoWorkerPool(appConfProvider.Get().WorkersNum),
		trigger:         trigger,
		lockService:     redisClient,
		bucketGetter:    redisClient,
		appConfProvider: appConfProvider,
		minuteBuckets:   make(map[string]int),
	}
}

// 得到最大桶数,根据迁移器检测到的数据规模，结合数学模型计算桶id
func (w *Worker) getValidBucket(ctx context.Context) int {
	now := time.Now()
	// 删除上一分钟的数据
	delete(w.minuteBuckets, now.Add(-time.Minute).Format(consts.MinuteFormat))

	// 复用一分钟内的数据
	bucket, ok := w.minuteBuckets[now.Format(consts.MinuteFormat)]
	if ok {
		return bucket
	}

	// 更新入map进行复用
	defer func() {
		w.minuteBuckets[now.Format(consts.MinuteFormat)] = bucket
	}()

	bucket = w.appConfProvider.Get().BucketsNum
	bucketKey := utils.GetBucketCntKey(now.Format(consts.MinuteFormat))
	res, err := w.bucketGetter.Get(ctx, bucketKey)
	if err != nil {
		log.ErrorContextf(ctx, "[scheduler] get bucket failed, key: %s, err:%v", bucketKey, err)
		return bucket
	}

	_bucket, err := strconv.Atoi(res)
	if err != nil {
		log.ErrorContextf(ctx, "[scheduler] get invalid bucket, key: %s, got:%v", bucketKey, res)
		return bucket
	}

	bucket = _bucket
	log.InfoContextf(ctx, "[scheduler] get valid bucket success, bucket: %d, cur: %v", _bucket, time.Now())

	return bucket
}

// 根据当前时间和桶id,获取一个分布式锁，开启一个触发器
func (w *Worker) asyncHandleSlice(ctx context.Context, t time.Time, bucketID int) {
	// GetDistributionLock : 可重入分布式锁，有key（string类型），token（线程和goroutine组成），client（redis.client）
	// GetTimeBucketLockKey : 当前时间和桶id拼成字符串 "time_bucket_lock_%s_%d", t.Format(consts.MinuteFormat), bucketID"
	locker := w.lockService.GetDistributionLock(utils.GetTimeBucketLockKey(t, bucketID))
	// 锁在redis中kv存储，k是key,v是token
	// TrtLockSeconds = 70,70s过期
	if err := locker.Lock(ctx, int64(w.appConfProvider.Get().TryLockSeconds)); err != nil {
		return
	}
	log.InfoContextf(ctx, "get scheduler lock success,key : %s", utils.GetTimeBucketLockKey(t, bucketID))

	ack := func() {
		// SuccessExpireSeconds = 130,锁任务执行成功，把锁过期时间更新为130s
		if err := locker.ExpireLock(ctx, int64(w.appConfProvider.Get().SuccessExpireSeconds)); err != nil {
			log.ErrorContextf(ctx, "expire lock failed,lock key: %s,err : %v", utils.GetTimeBucketLockKey(t, bucketID), err)
		}
	}

	if err := w.trigger.Work(ctx, utils.GetSliceMsgKey(t, bucketID), ack); err != nil {
		log.ErrorContextf(ctx, "triger work failed, err: %v", err)
	}
}

// 传入桶id,分别为一分钟前以及当下的桶开启一个异步处理函数（asyncHandleSlice）
func (w *Worker) handleSlice(ctx context.Context, bucketID int) {
	now := time.Now()
	if err := w.pool.Submit(func() {
		w.asyncHandleSlice(ctx, now.Add(-time.Minute), bucketID)
	}); err != nil {
		log.ErrorContextf(ctx, "[handle slice] submit task failed,err : %v", err)
	}

	if err := w.pool.Submit(func() {
		w.asyncHandleSlice(ctx, now, bucketID)
	}); err != nil {
		log.ErrorContextf(ctx, "[handle slice] submit task failed,err : %v", err)
	}
}

// 得到每一个桶id并且开启handleSlice,范围： [0:getValidBucket(ctx)]
func (w *Worker) handleSlices(ctx context.Context) {
	for i := 0; i < w.getValidBucket(ctx); i++ {
		w.handleSlice(ctx, i)
	}
}

// 执行流程：
// 1. 首先开启触发器的Start->开启执行器的Start:提前两s将任务从redis缓存到节点内存（map映射:timers map[uint]*vo.Timer）
// 2. 创建一个新的 Ticker 对象，每隔 100 毫秒 进行一次 handleSlices
func (w *Worker) Start(ctx context.Context) error {
	w.trigger.Start(ctx)

	//TryLockGapMilliSeconds = 100,调度器每次尝试获取分布式锁的时间间隔
	//0.1s 执行 一次
	ticker := time.NewTicker(time.Duration(w.appConfProvider.Get().TryLockGapMilliSeconds) * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
		select {
		case <-ctx.Done():
			log.WarnContext(ctx, "stopped")
			return nil
		default:
		}

		w.handleSlices(ctx)
	}
	return nil
}
