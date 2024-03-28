package webserver

import (
	"context"
	"fmt"
	"github.com/gin-gonic/gin"
	"gotimer_web/common/model/vo"
	service "gotimer_web/service/webserver"
	"net/http"
)

type timerService interface {
	CreateTimer(ctx context.Context, timer *vo.Timer) (uint, error)
	DeleteTimer(ctx context.Context, app string, id uint) error
	UpdateTimer(ctx context.Context, timer *vo.Timer) error
	GetTimer(ctx context.Context, id uint) (*vo.Timer, error)
	EnableTimer(ctx context.Context, app string, id uint) error
	UnableTimer(ctx context.Context, app string, id uint) error
	GetAppTimers(ctx context.Context, req *vo.GetAppTimersReq) ([]*vo.Timer, int64, error)
	GetTimersByName(ctx context.Context, req *vo.GetTimersByNameReq) ([]*vo.Timer, int64, error)
}

type TimerAPP struct {
	service timerService
}

func NewTimerApp(service *service.TimerService) *TimerAPP { return &TimerAPP{service: service} }

func (t *TimerAPP) CreateTimer(c *gin.Context) {
	var req vo.Timer
	if err := c.BindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, vo.NewCodeMsg(-1, fmt.Sprintf("[create timer] bind req failed,err: %v", err)))
		return
	}

	id, err := t.service.CreateTimer(c.Request.Context(), &req)
	if err != nil {
		c.JSON(http.StatusOK, vo.NewCodeMsg(-1, err.Error()))
		return
	}
	c.JSON(http.StatusOK, vo.NewCreateTimersResp(id, vo.NewCodeMsgWithErr(nil)))
}

func (t *TimerAPP) GetAppTimers(c *gin.Context) {
	var req vo.GetAppTimersReq
	if err := c.Bind(&req); err != nil {
		c.JSON(http.StatusBadRequest, vo.NewCodeMsg(-1, fmt.Sprintf("[get app timers] bind req failed, err: %v", err)))
		return
	}

	timers, total, err := t.service.GetAppTimers(c.Request.Context(), &req)
	if err != nil {
		c.JSON(http.StatusOK, vo.NewCodeMsg(-1, err.Error()))
		return
	}
	c.JSON(http.StatusOK, vo.NewGetTimersResp(timers, total, vo.NewCodeMsgWithErr(nil)))
}

func (t *TimerAPP) GetTimersByName(c *gin.Context) {
	var req vo.GetTimersByNameReq
	if err := c.Bind(&req); err != nil {
		c.JSON(http.StatusBadRequest, vo.NewCodeMsg(-1, fmt.Sprintf("[get timers by name] bind req failed, err: %v", err)))
		return
	}

	timers, total, err := t.service.GetTimersByName(c.Request.Context(), &req)
	if err != nil {
		c.JSON(http.StatusOK, vo.NewCodeMsg(-1, err.Error()))
		return
	}
	c.JSON(http.StatusOK, vo.NewGetTimersResp(timers, total, vo.NewCodeMsgWithErr(nil)))
}

func (t *TimerAPP) DeleteTimer(c *gin.Context) {
	var req vo.TimerReq
	if err := c.BindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, vo.NewCodeMsg(-1, fmt.Sprintf("[delete timer] bind req failed, err: %v", err)))
		return
	}

	if err := t.service.DeleteTimer(c.Request.Context(), req.App, req.ID); err != nil {
		c.JSON(http.StatusOK, vo.NewCodeMsg(-1, err.Error()))
		return
	}
	c.JSON(http.StatusOK, vo.NewCodeMsgWithErr(nil))
}

func (t *TimerAPP) UpdateTimer(c *gin.Context) {
	c.JSON(http.StatusOK, nil)
}

func (t *TimerAPP) GetTimer(c *gin.Context) {
	var req vo.TimerReq
	if err := c.Bind(&req); err != nil {
		c.JSON(http.StatusBadRequest, vo.NewCodeMsg(-1, fmt.Sprintf("[get timer] bind req failed, err: %v", err)))
		return
	}

	timer, err := t.service.GetTimer(c.Request.Context(), req.ID)
	if err != nil {
		c.JSON(http.StatusOK, vo.NewCodeMsg(-1, err.Error()))
		return
	}
	c.JSON(http.StatusOK, vo.NewGetTimerResp(timer, vo.NewCodeMsgWithErr(nil)))
}

func (t *TimerAPP) EnableTimer(c *gin.Context) {
	var req vo.TimerReq
	if err := c.BindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, vo.NewCodeMsg(-1, fmt.Sprintf("[enable timer] bind req failed,err: %v", err)))
		return
	}

	if err := t.service.EnableTimer(c.Request.Context(), req.App, req.ID); err != nil {
		c.JSON(http.StatusOK, vo.NewCodeMsg(-1, err.Error()))
		return
	}
	c.JSON(http.StatusOK, vo.NewCodeMsgWithErr(nil))
}

func (t *TimerAPP) UnableTimer(c *gin.Context) {
	var req vo.TimerReq
	if err := c.BindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, vo.NewCodeMsg(-1, fmt.Sprintf("[enable timer] bind req failed, err:%v", err)))
		return
	}
	if err := t.service.UnableTimer(c.Request.Context(), req.App, req.ID); err != nil {
		c.JSON(http.StatusOK, vo.NewCodeMsg(-1, err.Error()))
		return
	}
	c.JSON(http.StatusOK, vo.NewCodeMsgWithErr(nil))
}
