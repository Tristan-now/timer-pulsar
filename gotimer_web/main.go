package main

import (
	"gotimer_web/app"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"syscall"
)

func main() {
	migratorApp := app.GetMigratorApp()
	//schedulerApp := app.GetSchedulerApp()
	webServer := app.GetWebServer()

	migratorApp.Start()
	//schedulerApp.Start()
	//defer schedulerApp.Stop()

	webServer.Start()

	//go func() {
	//	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {})
	//	_ = http.ListenAndServe(":9999", nil)
	//}()
	go func() {
		log.Println(http.ListenAndServe(":6060", nil))
	}()
	runtime.SetBlockProfileRate(1)     // 开启对阻塞操作的跟踪，block
	runtime.SetMutexProfileFraction(1) // 开启对锁调用的跟踪，mutex

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT)
	<-quit
}
