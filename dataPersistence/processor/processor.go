package processor

import (
	"github.com/Shopify/sarama"
)

type Processor interface {
	Process(msg *sarama.ConsumerMessage)
}
