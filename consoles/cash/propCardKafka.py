# -*- coding: utf-8 -*-
# 资讯板块统计

from common.funcs  import funcs, printf, mongoStats
from models.NewsBaseStats import newsBase
from cfg import cfg
import time,binascii,json
from datetime import date, datetime
from pymongo import MongoClient,ReadPreference,DESCENDING
from bson.objectid import ObjectId
from kafka import KafkaConsumer

class PropCardKafka(object):
	kafkabrokers = cfg.kafka.get('cashseed.host')
	kafkaTopic = 'prop_card_complete'
	dbName = 'prop_card'
	mongoHost = cfg.mongodb.get('cashseed.host')

	def subscribe(self, data):
		brokers = self.kafkabrokers.split(',')
		consumer = KafkaConsumer(client_id='propcard_earlier_client',
                                 group_id='prop_card_finish_group3',
                                 bootstrap_servers=brokers,
                                 value_deserializer=lambda v: v.decode('utf-8'),
                                 #consumer_timeout_ms=120000, # StopIteration if no message after 30sec
                                 auto_offset_reset='earliest',
                                 enable_auto_commit=True)
		client = MongoClient(self.mongoHost)
		db = client[self.dbName]
        # 订阅话题
		consumer.subscribe(self.kafkaTopic)
		for message in consumer:
			print('partition:{}, offset:{}'.format(message.partition, message.offset))
			print(message)

			try:
				value = json.loads(message.value)
			except:
				time.sleep(1)
				continue

			optype = value.get('type','complete')
			if optype == 'complete':
				if not value.get('tvmid') or not value.get('card_id') or not value.get('last_use_time'):
					continue
				tvmid = value['tvmid']
				card_id = value['card_id']
				ctime = value['last_use_time']
				collectionName = 'user_prop_{}'.format(binascii.crc32(tvmid.encode('utf-8')) % 50)
				flag = db[collectionName].update_one({'tvmid':tvmid,'card_id':card_id}, {'$set':{'last_use_time':ctime,'available_times':0}})
				print('operate:', flag.modified_count, ',collect:',collectionName)


propCardKafka = PropCardKafka()