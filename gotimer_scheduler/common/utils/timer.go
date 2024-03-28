package utils

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"gotimer_scheduler/common/consts"
)

func UnionTimerIDUnix(timeID uint, unix int64) string {
	return fmt.Sprintf("%d_%d", timeID, unix)
}

func SplitTimerIDUnix(str string) (uint, int64, error) {
	timerIDUnix := strings.Split(str, "_")
	if len(timerIDUnix) != 2 {
		return 0, 0, fmt.Errorf("invalid timerID unix str: %s", str)
	}

	timerID, _ := strconv.ParseInt(timerIDUnix[0], 10, 64)
	unix, _ := strconv.ParseInt(timerIDUnix[1], 10, 64)
	return uint(timerID), unix, nil
}

func GetTaskBloomFilterKey(timeStr string) string {
	return "task_bloom_" + timeStr
}

func GetBucketCntKey(key string) string {
	return "bucket_cnt_" + key
}

func GetTimeBucketLockKey(t time.Time, bucketID int) string {
	return fmt.Sprintf("time_bucket_lock_%s_%d", t.Format(consts.MinuteFormat), bucketID)
}

func GetMigratorLockKey(t time.Time) string {
	return fmt.Sprintf("migrator_lock_%s", t.Format(consts.HourFormat))
}

func GetMonitorLockKey(t time.Time) string {
	return fmt.Sprintf("monitor_lock_%s", t.Format(consts.MinuteFormat))
}

func GetSliceMsgKey(t time.Time, bucketID int) string {
	return fmt.Sprintf("%s_%d", t.Format(consts.MinuteFormat), bucketID)
}

func GetEnableLockKey(app string) string {
	return fmt.Sprintf("enable_timer_lock_%s", app)
}

func GetCreateLockKey(app string) string {
	return fmt.Sprintf("create_timer_lock_%s", app)
}

func SplitTimeBucket(key string) (time.Time, int, error) {
	timerBucket := strings.Split(key, "_")
	if len(timerBucket) != 2 {
		return time.Time{}, 0, fmt.Errorf("invalid time bucket key: %s", key)
	}

	t, err := time.ParseInLocation(consts.MinuteFormat, timerBucket[0], time.Local)
	if err != nil {
		return t, 0, err
	}

	bucket, err := strconv.Atoi(timerBucket[1])
	return t, bucket, err
}

func GetForwardTwoMigrateStepEnd(cur time.Time, diff time.Duration) time.Time {
	end := cur.Add(diff)
	return time.Date(end.Year(), end.Month(), end.Day(), end.Hour(), 0, 0, 0, time.Local)
}
