package main

import (
	"flag"
	"github.com/lessos/lessgo/logger"
	"os"
	"os/signal"
	"spark-collector/collector"
	"spark-collector/config"
	"spark-collector/controller"
	// "spark-collector/db"
	"spark-collector/scheduler"
	"spark-collector/web"
	// "strconv"
	// "log"
	"strings"
	// "time"
)

var (
	cfgPath  = flag.String("c", "config.json", "Config path")
	addr     = flag.String("http", "", "Http Server Address,Example:\":8081\"")
	onlyhttp = flag.Bool("onlyhttp", false, "Only Http Service,Default:false")
)

func init() {
	flag.Parse()
}

func main() {

	if *cfgPath == "" {
		panic("config is missing")
	}
	err := config.Init(*cfgPath)
	if err != nil {
		panic(err)
	}

	logger.LogDirSet(config.Cfg.LogPath)

	if *addr != "" {
		if *onlyhttp {
			web.Listen(*addr)
		} else {
			go web.Listen(*addr)
		}
	}

	brokers := strings.Split(config.Cfg.KafKaBrokers, ",")
	c, err := collector.NewCollector(brokers, config.Cfg.KafkaTopic, config.Cfg.KafkaGroup)
	if err != nil {
		panic(err)
	}
	sparkEntryProcessor := controller.SparkEntry()
	c.AddProcessor(sparkEntryProcessor)

	s := scheduler.NewScheduler()
	s.AddCollector(c)

	s.Schedule()

	signalChan := make(chan os.Signal)
	signal.Notify(signalChan)

	for {
		select {
		case s := <-signalChan:
			// collector.OffsetDump()
			panic("get signal:" + s.String())
			return
		}
	}
}
