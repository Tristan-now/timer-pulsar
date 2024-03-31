package main

import (
	"context"
	"fmt"
	"github.com/spf13/viper"
	cf "gotimer_executor/common/conf"
	"gotimer_executor/dao/task"
	"gotimer_executor/dao/timer"
	"gotimer_executor/pkg/bloom"
	"gotimer_executor/pkg/cron"
	"gotimer_executor/pkg/hash"
	"gotimer_executor/pkg/log"
	"gotimer_executor/pkg/mysql"
	"gotimer_executor/pkg/promethus"
	"gotimer_executor/pkg/redis"
	"gotimer_executor/pkg/xhttp"
	"gotimer_executor/service/executor"
	mg "gotimer_executor/service/migrator"
	"os"
	"sync"
)

var wg sync.WaitGroup

var defaultRedisConfProvider *cf.RedisConfigProvider
var defaultMysqlConf *cf.MysqlConfProvider
var defaultSchedulerConf *cf.SchedulerAppConfProvider
var defaultMigratorConf *cf.MigratorAppConfProvider

// 兜底配置
var gConf GloablConf = GloablConf{
	Migrator: &cf.MigratorAppConf{
		// 单节点并行协程数
		WorkersNum: 1000,
		// 每次迁移数据的时间间隔，单位：min
		MigrateStepMinutes: 60,
		// 迁移成功更新的锁过期时间，单位：min
		MigrateSucessExpireMinutes: 120,
		// 迁移器获取锁时，初设的过期时间，单位：min
		MigrateTryLockMinutes: 20,
		// 迁移器提前将定时器数据缓存到内存中的保存时间，单位：min
		TimerDetailCacheMinutes: 2,
	},
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
	Migrator  *cf.MigratorAppConf  `yaml:"migrator"`
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

	defaultSchedulerConf = cf.NewSchedulerAppConfProvider(gConf.Scheduler)
	defaultRedisConfProvider = cf.NewRedisConfigProvider(gConf.Redis)
	defaultMysqlConf = cf.NewMysqlConfProvider(gConf.Mysql)
	defaultMigratorConf = cf.NewMigratorAppConfProvider(gConf.Migrator)

	mysqlClient, err := mysql.GetClient(defaultMysqlConf)
	timerDao := timer.NewTimerDAO(mysqlClient)
	taskDao := task.NewTaskDAO(mysqlClient)
	timerService := executor.NewTimerService(timerDao, taskDao, defaultMigratorConf)

	jsonClient := xhttp.NewJSONClient()
	redisCLient := redis.GetClient(defaultRedisConfProvider)

	// encryptor1 *hash.SHA1Encryptor, encryptor2 *hash.Murmur3Encyptor
	e1 := hash.NewSHA1Encryptor()
	e2 := hash.NewMurmur3Encryptor()

	filter := bloom.NewFilter(redisCLient, e1, e2)
	rep := promethus.GetReporter()

	tashCache := task.NewTaskCache(redisCLient, defaultSchedulerConf)
	cronPr := cron.NewCronParser()
	migrateWoker := mg.NewWorker(timerDao, taskDao, tashCache, redisCLient, cronPr, defaultMigratorConf)

	go func() {
		fmt.Println("开始迁移")
		migrateWoker.MigrateNow(context.Background())
		migrateWoker.Start(context.Background())
		fmt.Println("迁移执行成功")
	}()

	executorWorker := executor.NewWorker(timerService, taskDao, jsonClient, filter, rep)

	for {
		msg, err := executorWorker.Consumer.Receive(context.Background())
		if err != nil {
			log.Errorf("executor msg get failed,%v", err)
		}
		fmt.Println("get msg : ", string(msg.Payload()))
		go func() {
			err = executorWorker.Work(context.Background(), string(msg.Payload()))
			if err != nil {
				log.Errorf("trigger failed,%v", err)
			}
			executorWorker.Consumer.Ack(msg)
			fmt.Println("ack done")
		}()
	}

	defer executorWorker.Consumer.Close()
	wg.Wait()
}
