package redis

import (
	"context"
	"errors"
	"github.com/gomodule/redigo/redis"
	"gotimer_web/common/utils"
)

const ftimerLockKeyPrefix = "FTIMER_LOCK_PREFIX_"

type DistributeLocker interface {
	Lock(context.Context, int64) error
	Unlock(context.Context) error
	ExpireLock(ctx context.Context, expireSeconds int64) error
}

/*
可重入式分布式锁通过在锁内部维护一个token来实现。
每次获取锁时，会检查token是否匹配当前线程或进程，如果匹配则直接返回，否则进行锁的获取操作。

相比一般的分布式锁，可重入式分布式锁在同一线程或进程多次获取同一把锁时不会阻塞，
因为它能够识别并允许这种行为。这样可以避免死锁情况的发生，提高了灵活性和易用性。
*/
type ReentrantDistributeLock struct {
	key    string
	token  string
	client *Client
}

func NewReentrantDistributeLock(key string, client *Client) *ReentrantDistributeLock {
	return &ReentrantDistributeLock{
		key:    key,
		token:  utils.GetProcessAndGoroutineIDStr(),
		client: client,
	}
}

func (r *ReentrantDistributeLock) Lock(ctx context.Context, expireSeconds int64) error {
	// 首先检查锁是不是属于自己，是则无事发生
	res, err := r.client.Get(ctx, r.key)
	if err != nil && !errors.Is(err, redis.ErrNil) {
		return err
	}
	if res == r.token {
		return nil
	}

	// 锁不属于自己
	reply, err := r.client.SetNX(ctx, r.getLockKey(), r.token, expireSeconds)
	if err != nil {
		return err
	}

	re, _ := reply.(int64)
	if re != 1 {
		return errors.New("lock is acquired by others")
	}
	return nil
}

func (r *ReentrantDistributeLock) Unlock(ctx context.Context) error {
	keyAndArgs := []interface{}{r.getLockKey(), r.token}
	reply, err := r.client.Eval(ctx, LuaCheckAndDeleteDistributionLock, 1, keyAndArgs)
	if err != nil {
		return err
	}

	if ret, _ := reply.(int64); ret != 1 {
		return errors.New("can`t unlock without ownership of lock")
	}
	return nil
}

// 更新锁的过期时间
func (r *ReentrantDistributeLock) ExpireLock(ctx context.Context, expireSeconds int64) error {
	keysAndArgs := []interface{}{r.getLockKey(), r.token, expireSeconds}
	reply, err := r.client.Eval(ctx, LuaCheckAndExpireDistributionLock, 1, keysAndArgs)
	if err != nil {
		return err
	}

	if ret, _ := reply.(int64); ret != 1 {
		return errors.New("can not expire lock without ownership of lock")
	}

	return nil
}

func (r *ReentrantDistributeLock) getLockKey() string {
	return ftimerLockKeyPrefix + r.key
}
