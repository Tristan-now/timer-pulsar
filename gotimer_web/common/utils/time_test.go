package utils

import (
	"testing"
	"time"
)

func TestGetStartHour(t *testing.T) {
	t1 := GetStartHour(time.Now())
	t.Log("GetStartHour :", t1)
}

func TestGetMinute(t *testing.T) {
	t1 := GetMinute(time.Now())
	t.Log("GetMinute :", t1)
}

func TestGetMinuteStr(t *testing.T) {
	t1 := GetMinuteStr(time.Now())
	t.Log("GetMinuteStr :", t1)
}

func TestGetDayStr(t *testing.T) {
	t1 := GetDayStr(time.Now())
	t.Log("GetDayStr :", t1)
}

func TestGetHourStr(t *testing.T) {
	t1 := GetHourStr(time.Now())
	t.Log("GetHourStr :", t1)
}

func TestGetStartMinute(t *testing.T) {
	t1, _ := GetStartMinute("00:00")
	t.Log("GetStartMinute :", t1)
}

func TestGetStartMinute2(t *testing.T) {
	t1, _ := GetStartMinute("00:01")
	t.Log("GetStartMinute :", t1)
}

func TestGetStartMinute3(t *testing.T) {
	t1, _ := GetStartMinute("01:00")
	t.Log("GetStartMinute :", t1)
}
