package service

import (
	"context"
	"fmt"
	"time"

	mconf "gotimer_executor/common/conf"
	"gotimer_executor/common/consts"
	"gotimer_executor/common/utils"
	taskdao "gotimer_executor/dao/task"
	timerdao "gotimer_executor/dao/timer"
	"gotimer_executor/pkg/cron"
	"gotimer_executor/pkg/log"
	"gotimer_executor/pkg/pool"
	"gotimer_executor/pkg/redis"
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

func (w *Worker) Start(ctx context.Context) error {
	fmt.Println("迁移start")
	conf := w.appConfigProvider.Get()
	ticker := time.NewTicker(time.Duration(conf.MigrateStepMinutes) * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		fmt.Println("迁移ticker")
		log.InfoContext(ctx, "migrator ticking...")
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		locker := w.lockService.GetDistributionLock(utils.GetMigratorLockKey(utils.GetStartHour(time.Now())))
		if err := locker.Lock(ctx, int64(conf.MigrateTryLockMinutes)*int64(time.Minute/time.Second)); err != nil {
			log.ErrorContext(ctx, "migrator get lock failed, key: %s, err: %v", utils.GetMigratorLockKey(utils.GetStartHour(time.Now())), err)
			continue
		}

		if err := w.Migrate(ctx); err != nil {
			log.ErrorContext(ctx, "Migrate failed, err: %v", err)
			continue
		}

		_ = locker.ExpireLock(ctx, int64(conf.MigrateSucessExpireMinutes)*int64(time.Minute/time.Second))
	}
	return nil
}

func (w *Worker) Migrate(ctx context.Context) error {
	timers, err := w.timerDAO.GetTimers(ctx, timerdao.WithStatus(int32(consts.Enabled.ToInt())))
	if err != nil {
		return err
	}

	conf := w.appConfigProvider.Get()
	now := time.Now()
	start, end := utils.GetStartHour(now.Add(time.Duration(conf.MigrateStepMinutes)*time.Minute)), utils.GetStartHour(now.Add(2*time.Duration(conf.MigrateStepMinutes)*time.Minute))
	// 迁移可以慢慢来，不着急
	for _, timer := range timers {
		nexts, _ := w.cronParser.NextsBetween(timer.Cron, start, end)
		if err := w.timerDAO.BatchCreateRecords(ctx, timer.BatchTasksFromTimer(nexts)); err != nil {
			log.ErrorContextf(ctx, "migrator batch create records for timer: %d failed, err: %v", timer.ID, err)
		}
		time.Sleep(5 * time.Second)
	}

	// if err := w.batchCreateBucket(ctx, start, end); err != nil {
	// 	log.ErrorContextf(ctx, "batch create bucket failed, start: %v", start)
	// 	return err
	// }

	// log.InfoContext(ctx, "migrator batch create db tasks susccess")
	return w.migrateToCache(ctx, start, end)
}

func (w *Worker) MigrateNow(ctx context.Context) error {
	fmt.Println("MigrateNow 初始化")
	timers, err := w.timerDAO.GetTimers(ctx, timerdao.WithStatus(int32(consts.Enabled.ToInt())))
	if err != nil {
		return err
	}

	conf := w.appConfigProvider.Get()
	now := time.Now()
	start := now
	end := utils.GetStartHour(now.Add(2 * time.Duration(conf.MigrateStepMinutes) * time.Minute))
	fmt.Println("start time = ", start)
	fmt.Println("end time = ", end)
	// 迁移可以慢慢来，不着急
	for _, timer := range timers {
		nexts, _ := w.cronParser.NextsBetween(timer.Cron, start, end)
		fmt.Println("nexts = ", nexts)
		if err := w.timerDAO.BatchCreateRecords(ctx, timer.BatchTasksFromTimer(nexts)); err != nil {
			log.ErrorContextf(ctx, "migrator batch create records for timer: %d failed, err: %v", timer.ID, err)
		}
		time.Sleep(5 * time.Second)
	}

	// if err := w.batchCreateBucket(ctx, start, end); err != nil {
	// 	log.ErrorContextf(ctx, "batch create bucket failed, start: %v", start)
	// 	return err
	// }

	// log.InfoContext(ctx, "migrator batch create db tasks susccess")
	return w.migrateToCache(ctx, start, end)
}

// func (w *Worker) batchCreateBucket(ctx context.Context, start, end time.Time) error {
// 	cntByMins, err := w.taskDAO.CountGroupByMinute(ctx, start.Format(consts.SecondFormat), end.Format(consts.SecondFormat))
// 	if err != nil {
// 		return err
// 	}

// 	return w.taskCache.BatchCreateBucket(ctx, cntByMins, end)
// }

func (w *Worker) migrateToCache(ctx context.Context, start, end time.Time) error {
	// 迁移完成后，将所有添加的 task 取出，添加到 redis 当中
	fmt.Println("migrateToCache")
	tasks, err := w.taskDAO.GetTasks(ctx, taskdao.WithStartTime(start), taskdao.WithEndTime(end))
	if err != nil {
		log.ErrorContextf(ctx, "migrator batch get tasks failed, err: %v", err)
		return err
	}
	// log.InfoContext(ctx, "migrator batch get tasks susccess")
	return w.taskCache.BatchCreateTasks(ctx, tasks, start, end)
}
