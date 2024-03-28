package executor

import (
	"context"
	"gotimer_web/common/conf"
	"gotimer_web/common/consts"
	"gotimer_web/common/model/po"
	"gotimer_web/common/model/vo"
	taskdao "gotimer_web/dao/task"
	timerdao "gotimer_web/dao/timer"
	"gotimer_web/pkg/log"
	"sync"
	"time"
)

type timerDAO interface {
	GetTimer(context.Context, ...timerdao.Option) (*po.Timer, error)
	GetTimers(ctx context.Context, opts ...timerdao.Option) ([]*po.Timer, error)
}

type TimerService struct {
	sync.Once
	confProvider *conf.MigratorAppConfProvider
	ctx          context.Context
	stop         func()
	timers       map[uint]*vo.Timer
	timerDAO     timerDAO
	taskDAO      *taskdao.TaskDAO
}

func NewTimerService(timerDAO *timerdao.TimerDAO, taskDAO *taskdao.TaskDAO, confProvider *conf.MigratorAppConfProvider) *TimerService {
	return &TimerService{
		confProvider: confProvider,
		timers:       make(map[uint]*vo.Timer),
		timerDAO:     timerDAO,
		taskDAO:      taskDAO,
	}
}

// 使用map去重，把po.Task的id放到切片里
func getTimersIDs(tasks []*po.Task) []uint {
	timerIDSet := make(map[uint]struct{})
	for _, task := range tasks {
		if _, ok := timerIDSet[task.TimerID]; ok {
			continue
		}
		timerIDSet[task.TimerID] = struct{}{}
	}
	timerIDS := make([]uint, 0, len(timerIDSet))
	for id := range timerIDSet {
		timerIDS = append(timerIDS, id)
	}
	return timerIDS
}

func getTimersMap(pTimers []*po.Timer) (map[uint]*vo.Timer, error) {
	vTimers, err := vo.NewTimers(pTimers)
	if err != nil {
		return nil, err
	}

	timers := make(map[uint]*vo.Timer, len(vTimers))
	for _, vTimers := range vTimers {
		timers[vTimers.ID] = vTimers
	}
	return timers, nil
}

// 由起始结束时间得到定时器的vo的map
func (t *TimerService) getTimersByTime(ctx context.Context, start, end time.Time) (map[uint]*vo.Timer, error) {
	tasks, err := t.taskDAO.GetTasks(ctx, taskdao.WithStartTime(start), taskdao.WithEndTime(end))
	if err != nil {
		return nil, err
	}

	timerIDs := getTimersIDs(tasks)
	if len(timerIDs) == 0 {
		return nil, nil
	}
	pTimers, err := t.timerDAO.GetTimers(ctx, timerdao.WithIDs(timerIDs), timerdao.WithStatus(int32(consts.Enabled)))
	if err != nil {
		return nil, err
	}
	return getTimersMap(pTimers)
}

func (t *TimerService) Start(ctx context.Context) {
	t.Do(func() {
		go func() {
			t.ctx, t.stop = context.WithCancel(ctx)
			//变量stepMinutes，它从一个配置提供程序获取值，
			//然后创建一个定时器，该定时器根据stepMinutes定义的间隔触发
			//定时器是一个通道，每隔设定时间会向通道中发送一个信号（一般是当前的时间 time.Now()）
			//如果不手动关闭 ticker就会在for range 下一直循环
			stepMinutes := t.confProvider.Get().TimerDetailCacheMinutes
			ticker := time.NewTicker(time.Duration(stepMinutes) * time.Minute)
			defer ticker.Stop()

			for range ticker.C {
				select {
				case <-t.ctx.Done():
					return
				default:
				}

				go func() {
					start := time.Now()
					//提前两秒将任务缓存到内存中
					t.timers, _ = t.getTimersByTime(ctx, start, start.Add(time.Duration(stepMinutes)*time.Minute))
				}()
			}
		}()
	})
}

func (t *TimerService) Stop() {
	t.stop()
}

func (t *TimerService) GetTimer(ctx context.Context, id uint) (*vo.Timer, error) {
	if vTimer, ok := t.timers[id]; ok {
		return vTimer, nil
	}
	log.WarnContext(ctx, "get timer from local cache failed,timerID : %d", id)

	timer, err := t.timerDAO.GetTimer(ctx, timerdao.WithID(id))
	if err != nil {
		return nil, err
	}
	return vo.NewTimer(timer)
}
