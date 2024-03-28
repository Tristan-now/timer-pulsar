package trigger

import (
	"context"
	"time"

	"gotimer_trigger/common/conf"
	"gotimer_trigger/common/consts"
	"gotimer_trigger/common/model/po"
	"gotimer_trigger/common/model/vo"
	dao "gotimer_trigger/dao/task"
)

type TaskService struct {
	confProvider *conf.SchedulerAppConfProvider
	cache        *dao.TaskCache
	dao          taskDAO
}

func NewTaskService(dao *dao.TaskDAO, cache *dao.TaskCache, confPrivder *conf.SchedulerAppConfProvider) *TaskService {
	return &TaskService{
		confProvider: confPrivder,
		dao:          dao,
		cache:        cache,
	}
}

func (t *TaskService) GetTasksByTime(ctx context.Context, key string, bucket int, start, end time.Time) ([]*vo.Task, error) {
	// 先走缓存
	if tasks, err := t.cache.GetTasksByTime(ctx, key, start.UnixMilli(), end.UnixMilli()); err == nil && len(tasks) > 0 {
		return vo.NewTasks(tasks), nil
	}

	// 倘若缓存 miss 再走 db
	tasks, err := t.dao.GetTasks(ctx, dao.WithStartTime(start), dao.WithEndTime(end), dao.WithStatus(int32(consts.NotRunned.ToInt())))
	if err != nil {
		return nil, err
	}

	maxBucket := t.confProvider.Get().BucketsNum
	var validTask []*po.Task
	for _, task := range tasks {
		if task.TimerID%uint(maxBucket) != uint(bucket) {
			continue
		}
		validTask = append(validTask, task)
	}

	return vo.NewTasks(validTask), nil
}

type taskDAO interface {
	GetTasks(ctx context.Context, opts ...dao.Option) ([]*po.Task, error)
}
