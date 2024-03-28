package utils

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
)

func GetCurrentProcessID() string {
	return strconv.Itoa(os.Getpid())
}

// GetCurrentGoroutineID 获取当前的协程ID
// 协程ID（goroutine ID）是在Go语言运行时（runtime）中分配的。每个协程都有一个唯一的ID，由Go运行时系统负责管理和分配。
func GetCurrentGoroutineID() string {
	buf := make([]byte, 128)
	buf = buf[:runtime.Stack(buf, false)]
	//fmt.Println("buf = ", buf)
	stackInfo := string(buf)
	//fmt.Println("stackinfo = ", stackInfo)
	return strings.TrimSpace(strings.Split(strings.Split(stackInfo, "[running]")[0], "goroutine")[1])
}

func GetProcessAndGoroutineIDStr() string {
	return fmt.Sprintf("%s_%s", GetCurrentProcessID(), GetCurrentGoroutineID())
}
