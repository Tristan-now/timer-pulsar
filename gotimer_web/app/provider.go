package app

import (
	"go.uber.org/dig"

	"gotimer_web/app/migrator"
	"gotimer_web/app/scheduler"
	"gotimer_web/app/webserver"
	"gotimer_web/common/conf"
	taskdao "gotimer_web/dao/task"
	timerdao "gotimer_web/dao/timer"
	"gotimer_web/pkg/bloom"
	"gotimer_web/pkg/cron"
	"gotimer_web/pkg/hash"
	"gotimer_web/pkg/mysql"
	"gotimer_web/pkg/promethus"
	"gotimer_web/pkg/redis"
	"gotimer_web/pkg/xhttp"
	executorservice "gotimer_web/service/executor"
	migratorservice "gotimer_web/service/migrator"
	schedulerservice "gotimer_web/service/scheduler"
	triggerservice "gotimer_web/service/trigger"
	webservice "gotimer_web/service/webserver"
)

var (
	container *dig.Container
)

func init() {
	container = dig.New()

	provideConfig(container)
	providePKG(container)
	provideDAO(container)
	provideService(container)
	provideApp(container)
}

func provideConfig(c *dig.Container) {
	c.Provide(conf.DefaultMysqlConfigProvider)
	c.Provide(conf.DefaultSchedulerAppConfProvider)
	c.Provide(conf.DefaultTriggerAppConfProvider)
	c.Provide(conf.DefaultWebServerAppConfProvider)
	c.Provide(conf.DefaultRedisConfigProvider)
	c.Provide(conf.DefaultMigratorAppConfProvider)
}

func providePKG(c *dig.Container) {
	c.Provide(bloom.NewFilter)
	c.Provide(hash.NewMurmur3Encryptor)
	c.Provide(hash.NewSHA1Encryptor)
	c.Provide(redis.GetClient)
	c.Provide(mysql.GetClient)
	c.Provide(cron.NewCronParser)
	c.Provide(xhttp.NewJSONClient)
	c.Provide(promethus.GetReporter)
}

func provideDAO(c *dig.Container) {
	c.Provide(timerdao.NewTimerDAO)
	c.Provide(taskdao.NewTaskDAO)
	c.Provide(taskdao.NewTaskCache)
}

func provideService(c *dig.Container) {
	c.Provide(migratorservice.NewWorker)
	c.Provide(migratorservice.NewWorker)
	c.Provide(webservice.NewTaskService)
	c.Provide(webservice.NewTimerService)
	c.Provide(executorservice.NewTimerService)
	c.Provide(executorservice.NewWorker)
	c.Provide(triggerservice.NewWorker)
	c.Provide(triggerservice.NewTaskService)
	c.Provide(schedulerservice.NewWorker)
}

func provideApp(c *dig.Container) {
	c.Provide(migrator.NewMigratorApp)
	c.Provide(webserver.NewTaskApp)
	c.Provide(webserver.NewTimerApp)
	c.Provide(webserver.NewServer)
	c.Provide(scheduler.NewWorkerApp)
}

func GetSchedulerApp() *scheduler.WorkerApp {
	var schedulerApp *scheduler.WorkerApp
	if err := container.Invoke(func(_s *scheduler.WorkerApp) {
		schedulerApp = _s
	}); err != nil {
		panic(err)
	}
	return schedulerApp
}

func GetWebServer() *webserver.Server {
	var server *webserver.Server
	if err := container.Invoke(func(_s *webserver.Server) {
		server = _s
	}); err != nil {
		panic(err)
	}
	return server
}

func GetMigratorApp() *migrator.MigratorApp {
	var migratorApp *migrator.MigratorApp
	if err := container.Invoke(func(_m *migrator.MigratorApp) {
		migratorApp = _m
	}); err != nil {
		panic(err)
	}
	return migratorApp
}
