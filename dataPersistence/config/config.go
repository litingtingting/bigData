package config

import (
	"fmt"
	"github.com/lessos/lessgo/encoding/json"
	// "os"
)

var (
	Cfg config
)

type config struct {
	LogPath     string `json:"log_path"`
	MongoDbHost string `json:"mongo_db_host"`
	// MongoDbName          string `json:"mongo_db_name"`
	KafKaBrokers string `json:"kafka_brokers"`
	KafkaTopic   string `json:"kafka_topic"`
	KafkaGroup   string `json:"kafka_group"`
	// GoldRedisAddr          string `json:"gold_redis_addr"`
	// GoldRedisAuth          string `json:"gold_redis_auth"`
	// SeedRedisAddr          string `json:"seed_redis_addr"`
	// SeedRedisAuth          string `json:"seed_redis_auth"`
	// KafkaCashseedStatGroup string `json:"kafka_cashseed_stat_group"`
	// MongoStatDbName        string `json:"mongo_stat_db_name"`
}

func Init(path string) error {
	if path == "" {
		return fmt.Errorf("Config path is empty!")
	}
	if err := json.DecodeFile(path, &Cfg); err != nil {
		return err
	}
	// if Cfg.LogPath == "" {
	// 	return fmt.Errorf("log_path is empty!")
	// }

	// if Cfg.MongoDbHost == "" {
	// 	return fmt.Errorf("mongo_db_host is empty!")
	// }
	// if Cfg.MongoDbName == "" {
	// 	return fmt.Errorf("mongo_db_name is empty!")
	// }

	// if Cfg.KafKaBrokersCashseed == "" {
	// 	return fmt.Errorf("kafka_brokers_cashseed is empty!")
	// }
	// if Cfg.KafkaTopicCashseed == "" {
	// 	return fmt.Errorf("kafka_topic_cashseed is empty!")
	// }
	// if Cfg.KafkaGroupCashseed == "" {
	// 	return fmt.Errorf("kafka_group_cashseed is empty!")
	// }

	// if Cfg.KafkaCashseedStatGroup == "" {
	// 	return fmt.Errorf("kafka_cashseed_stat_group is empty!")
	// }

	// if Cfg.MongoStatDbName == "" {
	// 	return fmt.Errorf("mongo_stat_db_name is empty!")
	// }
	return nil
}
