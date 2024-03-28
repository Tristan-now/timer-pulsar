package utils

import (
	"gotimer_web/common/consts"
	"time"
)

// 根据本地时区将时间转换成time类型
func GetStartMinute(timerStr string) (time.Time, error) {
	return time.ParseInLocation(consts.MinuteFormat, timerStr, time.Local)
}

func GetDayStr(t time.Time) string { return t.Format(consts.DayFormat) }

func GetHourStr(t time.Time) string { return t.Format(consts.HourFormat) }

func GetMinuteStr(t time.Time) string { return t.Format(consts.MinuteFormat) }

func GetStartHour(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, time.Local)
}

func GetMinute(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), 0, 0, t.Location())
}
