import sys
import json
from cfg import cfg 

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

from pyspark.sql import SQLContext, SparkSession
#from pyspark.sql.types import *


import time
#from datetime import datetime

from  pymongo import MongoClient

##start
#./bin/spark-submit --master "spark://10.105.12.254:7077"  \
#  --jars  external/kafka-0-8-assembly/target/spark-streaming-kafka-0-8-assembly_2.11-2.2.3-SNAPSHOT.jar  \
#  --packages org.mongodb.spark:mongo-spark-connector_2.11:2.2.3  \
#  --executor-memory 512m  --py-files  /root/apps/batch/bigdata.zip  \ 
#  --total-executor-cores  2    /root/apps/batch/bigdata/index.py  cashseed  actual

class cashseedTest(object):

	@staticmethod
	def index(data):
		print (M.mapFunc)
		print (data)


	@staticmethod
	def actual(data):
		appName = 'cashseedActual';
		sc = SparkContext(appName=appName)
		ssc = StreamingContext(sc, 2)

		ctx = SQLContext(sc)

		brokers = cfg.kafka.get('cashseed.host')
		topic = cfg.kafka.get("cashseed.topic")

		kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers, 'group.id': 'spark-group-01' })

		lines = kvs.map(lambda x: json.loads(x[1]))

		count1 = lines.map(lambda x: (x['act_type'], (x['num'], 1, [x['tvmid']] )))  \
			.reduceByKeyAndWindow(lambda x, y: (x[0]+y[0], x[1]+y[1] , list(set(x[2]+y[2])) ), None, 60, 60)

		count2 = lines.map(lambda x: (x['tvmid'], x['num']))  \
			.mapValues(lambda x: (x, 1))     \
			.reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1]))

		count3 = lines.map(lambda x: (x['tvmid']+'_'+str(x['act_type']), x['num'])) \
			.mapValues(lambda x: (x, 1))   \
			.reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1]))


		def storeMongo(rdd1, schema, collection, database='stats', mode='append'):
			print ('rdd1',rdd1.collect())
			if rdd1.isEmpty(): return
			df = ctx.createDataFrame(rdd1, schema)
			df.write.format("com.mongodb.spark.sql.DefaultSource").mode(mode).options(uri=cfg.mongodb.get("host"), database=database, collection=collection,replaceDocument=False).save()

		def storeMongo1(rdd):
			if rdd.isEmpty(): return
			host = cfg.mongodb.get('host')
			client = MongoClient(host)
			collect = rdd.collect()

			for x in collect:
				client['stats'].cashseed_tvmid_date_stat.update_one({'_id': x[0]}, {'$inc': {'cashseed': x[2], 'count': x[3]}, '$set': {'tvmid': x[1], 'date': x[4]}}, upsert=True)
			client.close()


		actTypeSchema = ['act_type', 'cashseed', 'pv', 'uv','date', 'minute']
		count1 = count1.map(lambda x: (x[0], x[1][0], x[1][1], len(x[1][2]), time.strftime('%Y-%m-%d'), time.strftime('%H:%M')))
		count1.pprint()
		count1.foreachRDD(lambda r: storeMongo(r, actTypeSchema, 'cashseed_acttype_stream_stat'))

		count2 = count2.map(lambda x: (x[0]+time.strftime('%Y-%m-%d'), x[0], x[1][0], x[1][1], time.strftime('%Y-%m-%d')))
		count2.pprint()
		count2.foreachRDD(storeMongo1)


		count3.pprint()

		ssc.start()
		ssc.awaitTermination()
