package task

import (
	"context"
	"fmt"
	"gotimer_web/common/model/po"
	"gotimer_web/pkg/mysql"
)

type TaskDAO struct {
	client *mysql.Client
}

func NewTaskDAO(client *mysql.Client) *TaskDAO {
	return &TaskDAO{
		client: client,
	}
}

func (t *TaskDAO) GetTask(ctx context.Context, opts ...Option) (*po.Task, error) {
	db := t.client.DB.WithContext(ctx)
	for _, opt := range opts {
		db = opt(db)
	}

	var tasks po.Task
	return &tasks, db.First(&tasks).Error
}

func (t *TaskDAO) GetTasks(ctx context.Context, opts ...Option) ([]*po.Task, error) {
	db := t.client.DB.WithContext(ctx)
	for _, opt := range opts {
		db = opt(db)
	}

	var tasks []*po.Task
	return tasks, db.Model(&po.Task{}).Scan(&tasks).Error
}

func (t *TaskDAO) UpdateTask(ctx context.Context, task *po.Task) error {
	return t.client.DB.WithContext(ctx).Updates(task).Error
}

// 使用这些选项来修改数据库查询，并计算符合条件的记录数量，返回数量和任何遇到的错误。
func (t *TaskDAO) Count(ctx context.Context, opts ...Option) (int64, error) {
	db := t.client.DB.WithContext(ctx).Model(&po.Task{})
	for _, opt := range opts {
		db = opt(db)
	}
	var cnt int64
	return cnt, db.Count(&cnt).Error
}

// 从数据库检索出起始和结束之前的所有任务，放进po的分钟任务结构体
func (t *TaskDAO) CountGroupByMinute(ctx context.Context, startTimeStr, endTimeStr string) ([]*po.MinuteTaskCnt, error) {
	_sql := fmt.Sprintf(SQLGetMinuteTaskCnt, startTimeStr, endTimeStr)
	var raws []*po.MinuteTaskCnt
	return raws, t.client.DB.Raw(_sql).Scan(&raws).Error
}
