import sys
import json
from cfg import cfg

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

from pymongo import MongoClient

from datetime import datetime, date

from common.funcs import funcs
##start
#./bin/spark-submit --master "spark://10.105.12.254:7077"  \
#  --jars  external/kafka-0-8-assembly/target/spark-streaming-kafka-0-8-assembly_2.11-2.2.3-SNAPSHOT.jar  \
#  --packages org.mongodb.spark:mongo-spark-connector_2.11:2.2.3  \
#  --executor-memory 512m  --py-files  /root/apps/batch/bigdata.zip  \ 
#  --total-executor-cores  2    /root/apps/batch/bigdata/index.py  cashseed  actual

class cashseed(object):

	@staticmethod
	def actual(data):

		appName = 'cashseedActual'
		sc = SparkContext(appName=appName)
		ssc = StreamingContext(sc, 2)

		brokers = cfg.kafka.get('cashseed.host')
		topic = cfg.kafka.get("cashseed.topic")

		kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})

		lines = kvs.map(lambda x: json.loads(x[1]))

		###store data
		def mapStore(rdd):
			rdd['ISOdate'] = datetime.utcfromtimestamp(rdd['time'])
			ext = json.loads(rdd.get('ext','{}'))
			del rdd['ext']
			return {**ext, **rdd}

		def foreachStore(rdd):
			if rdd.isEmpty(): return
			host = cfg.mongodb.get('host')
			client = MongoClient(host)
			db = client.source
			db.cashseed_trade_log.insert_many(rdd.collect(), False)
			client.close()
		
		lines.map(mapStore).foreachRDD(foreachStore)
		#####store data end

		### act_type stat
		def foreachCount1(rdd):
			if rdd.isEmpty(): return
			host = cfg.mongodb.get('host')
			client = MongoClient(host)
			db = client.stats

			for x in rdd.collect():
				updateData = {'cashseed': abs(x[1][0]), 'pv':x[1][1], 'uv':x[1][2]}
				temp = x[0].split('--')
				act_type = temp[0]
				dates = temp[1]
				times = funcs.date2time(dates)

				_id = '{act_type}_{time}'.format(act_type=act_type, time=times)
				db.cashseed_acttype_date_stat.update_one({'_id': _id}, {'$inc': updateData, '$set': {'act_type': act_type, 'date': dates }}, upsert=True)
			client.close()

		def mapCount1(x):
			r = funcs.redisConnect()
			dates = funcs.time2date(x['time'])
			key = 'spark-{date}-{act_type}-{tvmid}'.format(date=dates, tvmid=x['tvmid'],act_type=x['act_type'])
			a = r.getset(key, 1)
			r.expire(key, 24*3600)
			if a == '1':
				c = 0
			else:
				c = 1
			key = '{act_type}--{date}'.format( act_type=x['act_type'], date=dates )
			return (key, (x['num'], 1, c))

		lines.map(mapCount1)  \
			.reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1], x[2]+y[2]))  \
			.foreachRDD(foreachCount1)
		### act_type stat end

		### total stat BEGIN
		def foreachTotal(rdd):
			print ('total', rdd.collect())
			if rdd.isEmpty(): return
			host = cfg.mongodb.get('host')
			client = MongoClient(host)
			db = client.stats

			x = rdd.collect()
			x = x[0]
			updateData = {'cashseed': x[1][0], 'pv': x[1][1], 'uv': x[1][2]}
			_id = '{act_type}_{time}'.format(act_type='income', time=funcs.date2time(x[0]))
			db.cashseed_date_stat.update_one({'_id':_id}, {'$inc': updateData, '$set': {'type': 'income', 'date': x[0]}}, upsert=True)
			client.close()

		def mapTotal(x):
			r = funcs.redisConnect()
			dates = funcs.time2date(x['time'])
			key = 'spark-{date}-{tvmid}'.format(date=dates, tvmid=x['tvmid'])
			a = r.getset(key, 1)
			r.expire(key, 24*3600)
			if a == '1':
				c = 0
			else:
				c = 1
			return (dates, (x['num'], 1, c))

		lines.filter(lambda x: x['num'] > 0)   \
			.map(mapTotal)   \
			.reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1], x[2]+y[2]))  \
			.foreachRDD(foreachTotal)
		### total stat END

		ssc.start()
		ssc.awaitTermination()
