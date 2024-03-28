package migrator

import (
	"context"
	mconf "gotimer_web/common/conf"
	"gotimer_web/common/consts"
	"gotimer_web/common/utils"
	taskdao "gotimer_web/dao/task"
	timerdao "gotimer_web/dao/timer"
	"gotimer_web/pkg/cron"
	"gotimer_web/pkg/log"
	"gotimer_web/pkg/pool"
	"gotimer_web/pkg/redis"
	"time"
)

type Worker struct {
	timerDAO          *timerdao.TimerDAO
	taskDAO           *taskdao.TaskDAO
	taskCache         *taskdao.TaskCache
	cronParser        *cron.CronParser
	lockService       *redis.Client
	appConfigProvider *mconf.MigratorAppConfProvider
	pool              pool.WorkerPool
}

func NewWorker(timerDAO *timerdao.TimerDAO, taskDAO *taskdao.TaskDAO, taskCache *taskdao.TaskCache, lockService *redis.Client,
	cronParser *cron.CronParser, appConfigProvider *mconf.MigratorAppConfProvider) *Worker {
	return &Worker{
		pool:              pool.NewGoWorkerPool(appConfigProvider.Get().WorkersNum),
		timerDAO:          timerDAO,
		taskDAO:           taskDAO,
		taskCache:         taskCache,
		lockService:       lockService,
		cronParser:        cronParser,
		appConfigProvider: appConfigProvider,
	}
}

// start到end内的task： mysql -> redis
func (w *Worker) migrateToCache(ctx context.Context, start, end time.Time) error {
	tasks, err := w.taskDAO.GetTasks(ctx, taskdao.WithStartTime(start), taskdao.WithEndTime(end))
	if err != nil {
		log.ErrorContextf(ctx, "migrator batch get tasks failed,err: %v", err)
		return err
	}
	return w.taskCache.BatchCreateTasks(ctx, tasks, start, end)
}

// 完成数据迁移，目标任务时间：从现在开始的一分钟后到两分钟之间
// 首先遍历mysql中激活态的定时器
// 根据起始结束时间筛选激活态的定时器，创建对应的task并且迁移到 mysql
// 最后从mysql的task中根据start和end筛选任务，迁移到redis
func (w *Worker) migrate(ctx context.Context) error {
	timers, err := w.timerDAO.GetTimers(ctx, timerdao.WithStatus(int32(consts.Enabled.ToInt())))
	if err != nil {
		return err
	}

	conf := w.appConfigProvider.Get()
	now := time.Now()
	// 每次迁移数据的时间间隔，单位：min
	//MigrateStepMinutes: 60,
	//start等于一分钟后，end等于两分钟后
	start, end := utils.GetStartHour(now.Add(time.Duration(conf.MigrateStepMinutes)*time.Minute)), utils.GetStartHour(now.Add(2*time.Duration(conf.MigrateStepMinutes)*time.Minute))

	for _, timer := range timers {
		nexts, _ := w.cronParser.NextsBetween(timer.Cron, start, end)
		//将task切片从po -> mysq
		if err := w.timerDAO.BatchCreateRecords(ctx, timer.BatchTasksFromTimer(nexts)); err != nil {
			log.ErrorContextf(ctx, "migrator batch create records for timer: %d failed ,err: %v", timer.ID, err)
		}
		time.Sleep(5 * time.Second)
	}
	return w.migrateToCache(ctx, start, end)
}

func (w *Worker) Start(ctx context.Context) error {
	conf := w.appConfigProvider.Get()
	//      每次迁移数据的时间间隔，单位：min
	//		MigrateStepMinutes: 60,
	ticker := time.NewTicker(time.Duration(conf.MigrateStepMinutes) * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		log.InfoContext(ctx, "migrator ticking ... ")
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		locker := w.lockService.GetDistributionLock(utils.GetMigratorLockKey(utils.GetStartHour(time.Now())))
		if err := locker.Lock(ctx, int64(conf.MigrateTryLockMinutes)*int64(time.Minute/time.Second)); err != nil {
			log.WarnContext(ctx, "migrator get lock failed,key: %s,err: %v", utils.GetMigratorLockKey(utils.GetStartHour(time.Now())), err)
			continue
		}

		if err := w.migrate(ctx); err != nil {
			log.WarnContext(ctx, "migrate failed,err: %v", err)
			continue
		}
		////    迁移成功更新的锁过期时间，单位：min
		//		MigrateSucessExpireMinutes: 120,
		_ = locker.ExpireLock(ctx, int64(conf.MigrateSucessExpireMinutes)*int64(time.Minute/time.Second))
	}
	return nil
}
