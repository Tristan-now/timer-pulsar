package task

import (
	"gorm.io/gorm"
	"time"
)

type Option func(*gorm.DB) *gorm.DB

func WithTaskID(id uint) Option {
	return func(db *gorm.DB) *gorm.DB {
		return db.Where("timer_id = ?", id)
	}
}

func WithTimerID(timeID uint) Option {
	return func(db *gorm.DB) *gorm.DB {
		return db.Where("timer_id = ?", timeID)
	}
}

func WithRunTimer(runTimer time.Time) Option {
	return func(d *gorm.DB) *gorm.DB {
		return d.Where("run_timer = ?", runTimer)
	}
}

func WithStartTime(start time.Time) Option {
	return func(d *gorm.DB) *gorm.DB {
		return d.Where("run_timer >= ?", start)
	}
}

func WithEndTime(end time.Time) Option {
	return func(d *gorm.DB) *gorm.DB {
		return d.Where("run_timer < ?", end)
	}
}

func WithStatus(status int32) Option {
	return func(d *gorm.DB) *gorm.DB {
		return d.Where("status = ?", status)
	}
}

func WithStatuses(statuses []int32) Option {
	return func(d *gorm.DB) *gorm.DB {
		return d.Where("status IN ?", statuses)
	}
}

func WithAsc() Option {
	return func(d *gorm.DB) *gorm.DB {
		return d.Order("created_at ASC")
	}
}

func WithDesc() Option {
	return func(d *gorm.DB) *gorm.DB {
		return d.Order("run_timer DESC")
	}
}

// offset表示返回之前要跳过的记录数
func WithPageLimit(offset, limit int) Option {
	return func(d *gorm.DB) *gorm.DB {
		return d.Offset(offset).Limit(limit)
	}
}
