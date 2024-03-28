package main

import (
	"context"
	"fmt"
	"github.com/spf13/viper"
	cf "gotimer_trigger/common/conf"
	"gotimer_trigger/dao/task"
	"gotimer_trigger/pkg/log"
	"gotimer_trigger/pkg/mysql"
	"gotimer_trigger/pkg/redis"
	"gotimer_trigger/service/trigger"
	"os"
	"sync"
)

var wg sync.WaitGroup

var defaultRedisConfProvider *cf.RedisConfigProvider
var defaultTriggerAppConfProvider *cf.TriggerAppConfProvider
var defaultMysqlConf *cf.MysqlConfProvider
var defaultSchedulerConf *cf.SchedulerAppConfProvider

// 兜底配置
var gConf GloablConf = GloablConf{
	Scheduler: &cf.SchedulerAppConf{
		// 单节点并行协程数
		WorkersNum: 100,
		// 分桶数量
		BucketsNum: 10,
		// 调度器获取分布式锁时初设的过期时间，单位：s
		TryLockSeconds: 70,
		// 调度器每次尝试获取分布式锁的时间间隔，单位：s
		TryLockGapMilliSeconds: 100,
		// 时间片执行成功后，更新的分布式锁时间，单位：s
		SuccessExpireSeconds: 130,
	},

	Trigger: &cf.TriggerAppConf{
		// 触发器轮询定时任务 zset 的时间间隔，单位：s
		ZRangeGapSeconds: 1,
		// 并发协程数
		WorkersNum: 10000,
	},

	Redis: &cf.RedisConfig{
		Network: "tcp",
		// 最大空闲连接数
		MaxIdle: 2000,
		// 空闲连接超时时间，单位：s
		IdleTimeoutSeconds: 30,
		// 连接池最大存活的连接数
		MaxActive: 1000,
		// 当连接数达到上限时，新的请求是等待还是立即报错
		Wait: true,
	},
	Mysql: &cf.MySQLConfig{
		MaxOpenConns: 100,
		MaxIdleConns: 50,
	},
}

type GloablConf struct {
	Scheduler *cf.SchedulerAppConf `yaml:"scheduler"`
	Redis     *cf.RedisConfig      `yaml:"redis"`
	Trigger   *cf.TriggerAppConf   `yaml:"trigger"`
	Mysql     *cf.MySQLConfig      `yaml:"mysql"`
}

func main() {
	wg.Add(1)

	path, err := os.Getwd()
	if err != nil {
		panic(err)
	}

	config := viper.New()
	config.AddConfigPath(path)   //设置读取的文件路径
	config.SetConfigName("conf") //设置读取的文件名
	config.SetConfigType("yaml") //设置文件的类型
	// 尝试进行配置读取
	if err := config.ReadInConfig(); err != nil {
		panic(err)
	}
	if err := config.Unmarshal(&gConf); err != nil {
		panic(err)
	}

	fmt.Println("config init ")
	defaultMysqlConf = cf.NewMysqlConfProvider(gConf.Mysql)
	mysqlClient, err := mysql.GetClient(defaultMysqlConf)

	if err != nil {
		log.Errorf(fmt.Sprintf("mysql conf init failed,%v", err))
	}
	fmt.Println("mysqlclient init ")
	taskDao := task.NewTaskDAO(mysqlClient)
	defaultRedisConfProvider = cf.NewRedisConfigProvider(gConf.Redis)
	redisClient := redis.GetClient(defaultRedisConfProvider)
	defaultSchedulerConf = cf.NewSchedulerAppConfProvider(gConf.Scheduler)
	taskCache := task.NewTaskCache(redisClient, defaultSchedulerConf)

	//func NewTaskService(dao *task.TaskDAO, cache *task.TaskCache, confPrivder *conf.SchedulerAppConfProvider)
	taskService := trigger.NewTaskService(taskDao, taskCache, defaultSchedulerConf)
	fmt.Println("taskservice init ")
	defaultTriggerAppConfProvider = cf.NewTriggerAppConfProvider(gConf.Trigger)
	triggerWorker := trigger.NewWorker(taskService, redisClient, defaultTriggerAppConfProvider)

	for {
		msg, err := triggerWorker.Consumer.Receive(context.Background())
		if err != nil {
			log.Errorf("trigger msg get failed,%v", err)
		}
		fmt.Println("get mes : ", string(msg.Payload()))
		go func() {
			err = triggerWorker.Work(context.Background(), string(msg.Payload()), func() {
				triggerWorker.Consumer.Ack(msg)
			})
			if err != nil {
				log.Errorf("trigger failed,%v", err)
			}
		}()
	}

	defer triggerWorker.Consumer.Close()
	wg.Wait()
}
