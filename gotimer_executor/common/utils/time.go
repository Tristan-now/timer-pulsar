package utils

import (
	"time"

	"gotimer_executor/common/consts"
)

func GetStartMinute(timeStr string) (time.Time, error) {
	return time.ParseInLocation(consts.MinuteFormat, timeStr, time.Local)
}

func GetDayStr(t time.Time) string {
	return t.Format(consts.DayFormat)
}

func GetHourStr(t time.Time) string {
	return t.Format(consts.HourFormat)
}

func GetMinuteStr(t time.Time) string {
	return t.Format(consts.MinuteFormat)
}

func GetStartHour(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, time.Local)
}

func GetMinute(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), 0, 0, t.Location())
}
