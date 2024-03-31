package utils

import (
	"fmt"
	//"gotimer_executor/common/utils"
	"testing"
	"time"
)

func TestGetStartHour(t *testing.T) {
	now := time.Now()
	start, end := GetStartHour(now.Add(time.Duration(60)*time.Minute)), GetStartHour(now.Add(2*time.Duration(60)*time.Minute))
	fmt.Println(start)
	fmt.Println(end)
}
