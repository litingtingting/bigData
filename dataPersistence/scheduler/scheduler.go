package scheduler

import (
	"spark-collector/collector"
	// . "spark-collector/config"
	// "github.com/Shopify/sarama"
	// "github.com/lessos/lessgo/logger"
	// kafkaCluster "github.com/bsm/sarama-cluster"
	// "io/ioutil"
	// "fmt"
	"log"
	// "os"
	// "time"
)

type Scheduler struct {
	collectors []*collector.Collector
}

func NewScheduler() *Scheduler {
	return &Scheduler{}

}

func (s *Scheduler) Schedule() {
	// _, err := os.Create("/web/log/kafka.log")
	// if err != nil {
	// 	panic(err)
	// }
	for _, c := range s.collectors {
		go func() {
			for err := range c.Consumer.Errors() {
				log.Printf("Error: %s\n", err.Error())
			}
		}()

		log.Println("Starting consume...")
		for {
			select {
			case partition, ok := <-c.Consumer.Partitions():
				log.Println(partition)
				log.Println(ok)
				if !ok {
					return
				}
				go c.Consume(partition.Messages())
			}
		}
	}

}

func (s *Scheduler) AddCollector(collector *collector.Collector) {
	s.collectors = append(s.collectors, collector)
}

// 定时保存offset
// func trickOffset() {
// 	c := time.Tick(1 * time.Second)
// 	for range c {
// 		collector.OffsetDump()
// 	}
// }
