package executor

import (
	"context"
	"encoding/json"
	"fmt"
	"gotimer_web/common/consts"
	"gotimer_web/common/model/vo"
	"gotimer_web/common/utils"
	taskdao "gotimer_web/dao/task"
	"gotimer_web/pkg/bloom"
	"gotimer_web/pkg/log"
	"gotimer_web/pkg/promethus"
	"gotimer_web/pkg/xhttp"
	nethttp "net/http"
	"strings"
	"time"
)

type Worker struct {
	timerService *TimerService
	taskDAO      *taskdao.TaskDAO
	httpClient   *xhttp.JSONClient
	bloomFilter  *bloom.Filter
	reporter     *promethus.Reporter
}

func NewWorker(timerService *TimerService, taskDAO *taskdao.TaskDAO, httpClient *xhttp.JSONClient, bloomFilter *bloom.Filter, reporter *promethus.Reporter) *Worker {
	return &Worker{
		timerService: timerService,
		taskDAO:      taskDAO,
		httpClient:   httpClient,
		bloomFilter:  bloomFilter,
		reporter:     reporter,
	}
}

// start作用  提前两秒将任务缓存到节点内存
func (w *Worker) Start(ctx context.Context) {
	w.timerService.Start(ctx)
}

/*
	执行器逻辑总结：
	最前面的输入：timerIDUnixKey ，包含了 定时器id 和 执行时间 两个信息
	分离出timerID 和 执行时间
	- 先进行布隆过滤器和定时器激活状态确认
	- 从缓存（如无去数据库找）提取任务，执行回调函数
	- 处理后事，更新数据库状态，记录监控数据

*/

func (w *Worker) Work(ctx context.Context, timerIDUnixKey string) error {
	timerID, unix, err := utils.SplitTimerIDUnix(timerIDUnixKey)
	if err != nil {
		return err
	}

	if exist, err := w.bloomFilter.Exist(ctx, utils.GetTaskBloomFilterKey(utils.GetDayStr(time.UnixMilli(unix))), timerIDUnixKey); err != nil || exist {
		log.WarnContext(ctx, "bloom filter check failed,start to check db,bloom key: %s,timerIDUnixKey: %s,err: %v,exist: %t", utils.GetTaskBloomFilterKey(utils.GetDayStr(time.UnixMilli(unix))), timerIDUnixKey, err, exist)

		task, err := w.taskDAO.GetTask(ctx, taskdao.WithTaskID(timerID), taskdao.WithRunTimer(time.UnixMilli(unix)))
		if err == nil && task.Status != consts.NotRunned.ToInt() {
			log.WarnContext(ctx, "task is already executed, timerID: %d,exec_time: %v", timerID, task.RunTimer)
			return nil
		}
	}
	return w.executeAndPostProcess(ctx, timerID, unix)
}

func (w *Worker) executeAndPostProcess(ctx context.Context, timerID uint, unix int64) error {
	timer, err := w.timerService.GetTimer(ctx, timerID)
	if err != nil {
		return fmt.Errorf("get timer failed,id: %d,err: %w", timerID, err)
	}

	if timer.Status != consts.Enabled {
		log.WarnContext(ctx, "timer has alread been unabled,timerID: %d", timerID)
		return nil
	}

	execTime := time.Now()
	resp, err := w.execute(ctx, timer)

	return w.postProcess(ctx, resp, err, timer.App, timerID, unix, execTime)
}

func (w *Worker) execute(ctx context.Context, timer *vo.Timer) (map[string]interface{}, error) {
	var (
		resp map[string]interface{}
		err  error
	)

	switch strings.ToUpper(timer.NotifyHTTPParam.Method) {
	case nethttp.MethodGet:
		err = w.httpClient.Get(ctx, timer.NotifyHTTPParam.URL, timer.NotifyHTTPParam.Header, nil, &resp)
	case nethttp.MethodPatch:
		err = w.httpClient.Patch(ctx, timer.NotifyHTTPParam.URL, timer.NotifyHTTPParam.Header, timer.NotifyHTTPParam.Body, &resp)
	case nethttp.MethodDelete:
		err = w.httpClient.Delete(ctx, timer.NotifyHTTPParam.URL, timer.NotifyHTTPParam.Header, timer.NotifyHTTPParam.Body, &resp)
	case nethttp.MethodPost:
		err = w.httpClient.Post(ctx, timer.NotifyHTTPParam.URL, timer.NotifyHTTPParam.Header, timer.NotifyHTTPParam.Body, &resp)
	default:
		err = fmt.Errorf("invalid http method: %s, timer: %s", timer.NotifyHTTPParam.Method, timer.Name)
	}
	return resp, err
}

// 处理后事的函数，记录监控数据、更新任务状态
func (w *Worker) postProcess(ctx context.Context, resp map[string]interface{}, execErr error, app string, timeID uint, unix int64, execTime time.Time) error {
	go w.reportMonitorData(app, unix, execTime)
	if err := w.bloomFilter.Set(ctx, utils.GetTaskBloomFilterKey(utils.GetDayStr(time.UnixMilli(unix))), utils.UnionTimerIDUnix(timeID, unix), consts.BloomFilterKeyExpireSeconds); err != nil {
		log.ErrorContext(ctx, "set bloom filter failed,key : %s,err: %v", utils.GetTaskBloomFilterKey(utils.GetDayStr(time.UnixMilli(unix))), err)
	}

	task, err := w.taskDAO.GetTask(ctx, taskdao.WithTimerID(timeID), taskdao.WithRunTimer(time.UnixMilli(unix)))
	if err != nil {
		return fmt.Errorf("get task failed,timerID : %d,runTimer: %d,err :%w", timeID, time.UnixMilli(unix), err)
	}

	respBody, _ := json.Marshal(resp)
	task.Output = string(respBody)

	if execErr != nil {
		task.Status = consts.Failed.ToInt()
	} else {
		task.Status = consts.Successed.ToInt()
	}

	return w.taskDAO.UpdateTask(ctx, task)
}

func (w *Worker) reportMonitorData(app string, expectExecTimeUnix int64, acutalExecTime time.Time) {
	w.reporter.ReportExecRecord(app)
	w.reporter.ReportTimerDelayRecord(app, float64(acutalExecTime.UnixMilli()-expectExecTimeUnix))
}
