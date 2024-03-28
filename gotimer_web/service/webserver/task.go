package webserver

import (
	"context"
	"gotimer_web/common/consts"
	"gotimer_web/common/model/vo"
	dao "gotimer_web/dao/task"
)

type TaskService struct {
	dao *dao.TaskDAO
}

func NewTaskService(dao *dao.TaskDAO) *TaskService {
	return &TaskService{
		dao: dao,
	}
}

// 输入task的id,从mysql中寻找对应的task返回为vo视图
func (t *TaskService) GetTask(ctx context.Context, id uint) (*vo.Task, error) {
	task, err := t.dao.GetTask(ctx, dao.WithTaskID(id))
	if err != nil {
		return nil, err
	}
	return vo.NewTask(task), nil
}

// 输入一个包含timerID的结构体，结合分页逻辑,返回这个timer对应的task
func (t *TaskService) GetTasks(ctx context.Context, req *vo.GetTasksReq) ([]*vo.Task, int64, error) {
	total, err := t.dao.Count(ctx, dao.WithTimerID(req.TimerID), dao.WithStatuses([]int32{
		int32(consts.Running),
		int32(consts.Successed),
		int32(consts.Failed),
	}))
	if err != nil {
		return nil, -1, err
	}

	offset, limit := req.Get()
	if total <= int64(offset) {
		return []*vo.Task{}, total, nil
	}
	tasks, err := t.dao.GetTasks(ctx, dao.WithTimerID(req.TimerID), dao.WithPageLimit(offset, limit), dao.WithStatuses([]int32{
		int32(consts.Running),
		int32(consts.Successed),
		int32(consts.Failed),
	}), dao.WithDesc())
	if err != nil {
		return nil, -1, err
	}

	return vo.NewTasks(tasks), total, nil
}
