package redis

import (
	"context"
	"errors"
	"github.com/gomodule/redigo/redis"
	"gotimer_web/common/conf"
	"gotimer_web/pkg/log"
	"time"
)

// Client Redis客户端
type Client struct {
	pool *redis.Pool
}

// GetClient 获取客户端
func GetClient(confProvider *conf.RedisConfigProvider) *Client {
	pool := getRedisPool(confProvider.Get())
	return &Client{
		pool: pool,
	}
}

func (c *Client) GetConn(ctx context.Context) (redis.Conn, error) { return c.pool.GetContext(ctx) }

func getRedisPool(config *conf.RedisConfig) *redis.Pool {
	return &redis.Pool{
		MaxIdle: config.MaxIdle,
		//释放空闲连接的时间阈值
		IdleTimeout: time.Duration(config.IdleTimeoutSeconds) * time.Second,
		//获取连接的函数
		Dial: func() (redis.Conn, error) {
			c, err := newRedisConn(config)
			if err != nil {
				return nil, err
			}
			return c, nil
		},
		MaxActive: config.MaxActive,
		Wait:      config.Wait,
		// 测试连接函数
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			if err != nil {
				log.Errorf("Failed to ping redis server,caused by %s", err)
			}
			return err
		},
	}
}

func newRedisConn(conf *conf.RedisConfig) (redis.Conn, error) {
	if conf.Address == "" {
		panic("Cannot get redis address from config")
	}

	conn, err := redis.DialContext(context.Background(),
		conf.Network, conf.Address, redis.DialPassword(conf.Password))
	if err != nil {
		log.Errorf("Failed to connect to redis,caused by %s", err)
		return nil, err
	}
	return conn, nil
}

// SetEx 执行 redis SET命令，expireTime 时间单位为秒
func (c *Client) SetEx(ctx context.Context, key, value string, expireSeconds int64) error {
	if key == "" || value == "" {
		return errors.New("redis SET key or value can`t be empty")
	}
	// 使用提供的上下文 来获取连接
	conn, err := c.pool.GetContext(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	_, err = conn.Do("SET", key, value, "EX", expireSeconds)
	return err

}
func (c *Client) SetNX(ctx context.Context, key, value string, expireSeconds int64) (interface{}, error) {
	if key == "" || value == "" {
		return -1, errors.New("redis SET keyNX or value can't be empty")
	}

	conn, err := c.pool.GetContext(ctx)
	if err != nil {
		return -1, err
	}
	defer conn.Close()

	reply, err := conn.Do("SETNX", key, value)
	if err != nil {
		return -1, err
	}

	r, _ := reply.(int64)
	if r == 1 {
		_, _ = conn.Do("EXPIRE", key, expireSeconds)
	}

	return reply, nil
}

// Eval  支持lua脚本
func (c *Client) Eval(ctx context.Context, src string, keyCount int, keysAndArgs []interface{}) (interface{}, error) {
	args := make([]interface{}, 2+len(keysAndArgs))
	args[0] = src
	args[1] = keyCount
	copy(args[2:], keysAndArgs)
	/*
		`copy` 函数用于复制切片（slice）中的元素。它通常用于复制数组（array）、切片（slice）或字符串（string）中的元素。
		`copy` 函数是浅拷贝（shallow copy）。

		浅拷贝意味着 `copy` 函数只复制源切片中的值到目标切片中。
		如果这些值是指向内存地址的指针（例如，指向结构体或其他复杂类型的指针），那么复制的是指针的值，
		而不是指针指向的实际数据。因此，如果你修改了复制后的目标切片中的元素，它不会影响到源切片中的元素，
		但是如果被复制的元素本身是引用类型，并且你通过复制得到的切片修改了这些引用类型的数据，
		那么这种修改会影响到原始数据。

		如果你需要深拷贝，即完全独立的副本，你需要为每个元素调用一个深拷贝的函数或方法。
		对于结构体，你可以使用 `encoding/gob` 或 `json` 包来实现深拷贝，
		或者为结构体定义一个复制方法。对于切片，你可以创建一个新的切片，并将每个元素逐个复制到新切片中。
	*/

	conn, err := c.pool.GetContext(ctx)
	if err != nil {
		return -1, err
	}
	defer conn.Close()
	return conn.Do("EVAL", args...)
}

func (c *Client) Get(ctx context.Context, key string) (string, error) {
	conn, err := c.pool.GetContext(ctx)
	if err != nil {
		return "", err
	}
	defer conn.Close()
	return redis.String(conn.Do("GET", key))
}

func (c *Client) Exists(ctx context.Context, keys ...interface{}) (bool, error) {
	if len(keys) == 0 {
		return false, errors.New("redis Exists keys can`t be empty")
	}
	conn, err := c.pool.GetContext(ctx)
	if err != nil {
		return false, err
	}
	defer conn.Close()

	args := make([]interface{}, len(keys))
	for i := range keys {
		args[i] = keys[i]
	}
	return redis.Bool(conn.Do("exists", args...))
}

func (c *Client) HGet(ctx context.Context, table, key string) (string, error) {
	conn, err := c.pool.GetContext(ctx)
	if err != nil {
		return "", err
	}
	defer conn.Close()
	reply, err := conn.Do("HGET", table, key)
	if err != nil {
		return "", err
	}
	if reply == nil {
		return "", nil
	}
	// String帮助把执行结果转换成字符串
	return redis.String(reply, err)
}

func (c *Client) HSet(ctx context.Context, table, key, value string) error {
	conn, err := c.pool.GetContext(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	_, err = conn.Do("HSET", table, key, value)
	return err
}

func (c *Client) ZrangeByScore(ctx context.Context, table string, score1, score2 int64) ([]string, error) {
	conn, err := c.pool.GetContext(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	raws, err := redis.Values(conn.Do("ZRANGEBYSCORE", table, score1, score2))
	if err != nil {
		return nil, err
	}

	var res []string
	for _, raw := range raws {
		tmp, ok := raw.([]byte)
		if !ok {
			continue
		}
		res = append(res, string(tmp))
	}
	return res, nil
}

func (c *Client) ZAdd(ctx context.Context, table string, score int64, value interface{}) error {
	conn, err := c.pool.GetContext(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	_, err = conn.Do("ZADD", table, score, value)
	return err
}

func (c *Client) Expire(ctx context.Context, key string, expireSeconds int64) error {
	conn, err := c.pool.GetContext(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	_, err = conn.Do("EXPIRE", key, expireSeconds)
	return err
}

func NewSetCommand(args ...interface{}) *Command {
	return &Command{
		Name: "SET",
		Args: args,
	}
}
func NewZAddCommand(args ...interface{}) *Command {
	return &Command{
		Name: "ZADD",
		Args: args,
	}
}

func NewSetBitCommand(args ...interface{}) *Command {
	return &Command{
		Name: "SETBIT",
		Args: args,
	}
}

func NewExpireCommand(args ...interface{}) *Command {
	return &Command{
		Name: "EXPIRE",
		Args: args,
	}
}

type Command struct {
	Name string
	Args []interface{}
}

// 将多个命令视为一个事务执行
func (c *Client) Transaction(ctx context.Context, commands ...*Command) ([]interface{}, error) {
	if len(commands) == 0 {
		return nil, nil
	}

	conn, err := c.pool.GetContext(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	_ = conn.Send("MULTI")
	for _, command := range commands {
		_ = conn.Send(command.Name, command.Args...)
	}

	return redis.Values(conn.Do("EXEC"))
}
func (c *Client) SetBit(ctx context.Context, key string, offset int32) (bool, error) {
	conn, err := c.pool.GetContext(ctx)
	if err != nil {
		return false, err
	}
	defer conn.Close()

	reply, err := redis.Int(conn.Do("SETBIT", key, offset, 1))
	return reply == 1, err
}

func (c *Client) GetBit(ctx context.Context, key string, offset int32) (bool, error) {
	conn, err := c.pool.GetContext(ctx)
	if err != nil {
		return false, err
	}
	defer conn.Close()
	reply, err := redis.Int(conn.Do("GETBIT", key, offset))
	return reply == 1, err
}

// MGET 是 Redis 中用于获取多个键对应的值的命令。给定多个键，它会返回这些键对应的值。
func (c *Client) MGet(ctx context.Context, keys ...interface{}) ([]string, error) {
	if len(keys) == 0 {
		return nil, errors.New("redis MSET args can't be nil or empty")
	}
	conn, err := c.pool.GetContext(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	args := make([]interface{}, len(keys))
	for i := range keys {
		args[i] = keys[i]
	}
	return redis.Strings(conn.Do("MGET", args...))
}

func (c *Client) GetDistributionLock(key string) DistributeLocker {
	return NewReentrantDistributeLock(key, c)
}
