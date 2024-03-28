package webserver

import (
	"context"
	"fmt"
	"github.com/gin-gonic/gin"
	"gotimer_web/common/model/vo"
	service "gotimer_web/service/webserver"
	"net/http"
)

type taskService interface {
	GetTask(ctx context.Context, id uint) (*vo.Task, error)
	GetTasks(ctx context.Context, req *vo.GetTasksReq) ([]*vo.Task, int64, error)
}

type TaskApp struct {
	service taskService
}

func NewTaskApp(service *service.TaskService) *TaskApp {
	return &TaskApp{
		service: service,
	}
}

func (t *TaskApp) GetTasks(c *gin.Context) {
	var req vo.GetTasksReq
	if err := c.Bind(&req); err != nil {
		c.JSON(http.StatusBadRequest, vo.NewCodeMsg(-1, fmt.Sprintf("[get tasks] bind req failed,err: %v", err)))
		return
	}

	tasks, total, err := t.service.GetTasks(c.Request.Context(), &req)
	c.JSON(http.StatusOK, vo.NewGetTasksResp(tasks, total, vo.NewCodeMsgWithErr(err)))
}
