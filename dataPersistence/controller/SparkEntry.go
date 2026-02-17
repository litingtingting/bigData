package controller

import (
	"github.com/Shopify/sarama"
	"github.com/lessos/lessgo/encoding/json"
	"github.com/lessos/lessgo/logger"
	// "log"
	"spark-collector/db"
	"spark-collector/processor"
	"strconv"
	"time"
)

type sparkProcessor struct{}

type Entry struct {
	Db         string      `json:"db"`
	Collection string      `json:"collection"`
	Data       interface{} `json:"data"`
}

func SparkEntry() processor.Processor {
	return &sparkProcessor{}
}

func (p *sparkProcessor) Process(msg *sarama.ConsumerMessage) {
	msgValue := string(msg.Value)
	// logger.Printf("info", "value:%s,offset:%d,partition:%d,topic:%s", msgValue, msg.Offset, msg.Partition, msg.Topic)

	defer func() {
		if err := recover(); err != nil {
			logger.Print("fatal", err)
		}
	}()

	var entry Entry
	err := json.Decode(msg.Value, &entry)
	if err != nil {
		logger.Printf("error", "%s json decode error:%s", msgValue, err.Error())
		return
	}

	entryData := entry.Data.(map[string]interface{})

	if entryData["time"] != nil {
		entryData["ISOdate"] = unixToUTC(entryData["time"].(string))
		entryData["time"], _ = strconv.Atoi(entryData["time"].(string))
	} else {
		entryData["ISOdate"] = time.Unix(time.Now().Unix(), int64(time.Now().Nanosecond()))
	}
	mongoClient := db.GetMgoConn(false)

	// fix stupid bug
	if entry.Db == "userbehavior" {
		entry.Db = "user_behavior"
	}

	// fix second stupid bug
	if entry.Collection == "mc_page_load_zixun" {
		entry.Collection = "mc_page_load_zixun1"
	}

	err = mongoClient.DB(entry.Db).C(entry.Collection).Insert(entryData)
	if err != nil {
		mongoClient := db.GetMgoConn(true)
		mongoClient.DB(entry.Db).C(entry.Collection).Insert(entryData)
		logger.Printf("error", "%s mongo insert error:%s", msgValue, err.Error())
		return
	} else {
		logger.Printf("info", "success to insert mongo,data:%s", msgValue)
	}
}

func unixToUTC(tm string) time.Time {
	return time.Unix(splitTime(tm)).UTC()
}

func splitTime(timeStr string) (int64, int64) {
	// timeStr := strconv.Itoa(tm)
	sec, _ := strconv.Atoi(timeStr[:10])
	micro, _ := strconv.Atoi(timeStr[10:])
	return int64(sec), int64(micro * 1000000)
}
