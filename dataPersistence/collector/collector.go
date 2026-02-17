package collector

import (
	"github.com/Shopify/sarama"
	kafkaCluster "github.com/bsm/sarama-cluster"
	"spark-collector/processor"
	// "github.com/lessos/lessgo/logger"
	"log"
)

// type CollectorInterface interface {
// 	Consume()
// }

func NewCollector(addrs []string, topic string, group string) (*Collector, error) {
	config := kafkaCluster.NewConfig()
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Group.Mode = kafkaCluster.ConsumerModePartitions
	// config.

	// init consumer
	topics := []string{topic}
	log.Println(addrs)
	log.Println(topics)
	consumer, err := kafkaCluster.NewConsumer(addrs, group, topics, config)
	if err != nil {
		return nil, err
	}

	// consumer, err := sarama.NewConsumer(addrs, config)
	// if err != nil {
	// 	return nil, err
	// }
	return &Collector{Consumer: consumer, Topic: topic}, nil
}

type Collector struct {
	Consumer   *kafkaCluster.Consumer
	processors []processor.Processor
	Topic      string
}

func (c *Collector) Consume(msgChannel <-chan *sarama.ConsumerMessage) {
	for msg := range msgChannel {
		// log.Println(string(msg.Value))
		// logger.Print("info", string(msg.Value))
		// logger.Printf("info", "partition:%d, offset:%d", msg.Partition, msg.Offset)
		for _, p := range c.processors {
			p.Process(msg)
		}
		c.Consumer.MarkOffset(msg, "")
	}
}

func (c *Collector) AddProcessor(processor processor.Processor) {
	c.processors = append(c.processors, processor)
}
