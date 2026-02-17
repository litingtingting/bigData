"""
./bin/spark-submit --master "spark://10.105.12.254:7077" \
--jars  external/kafka-0-8-assembly/target/spark-streaming-kafka-0-8-assembly_2.11-2.2.3-SNAPSHOT.jar \
--packages org.mongodb.spark:mongo-spark-connector_2.11:2.2.3  \
--executor-memory 512m
--py-files  /root/apps/batch/bigdata.zip   \
 /root/apps/batch/bigdata/cashseedKafkaToMongo.py  
"""




from __future__ import print_function


from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

from pyspark.sql import SQLContext
from pyspark.sql.types import *

from cfg import cfg
import sys
import json

from time import time
from datetime import datetime


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: cashseedKafkaToMongo.py <title> ", file=sys.stderr)
        sys.exit(-1)

    title = sys.argv[1]

    option = getattr(cfg, title)
    database = option.get("database")
    collection = option.get("collection")
    dataFrame = option.get("dataFrame")

    sc = SparkContext(appName="cashseedKafkaToMongo")

    ctx = SQLContext(sc)
    ssc = StreamingContext(sc, 2)

    brokers = cfg.kafka.get('cashseed.host')
    topic = cfg.kafka.get("cashseed.topic")

    fieldArr = []
    for fields in dataFrame.split(","):
        temp = fields.split("=")
        field_name = temp[0].strip()
        if len(temp) >= 2:
            field_type = temp[1] + "Type"
        else:
            field_type = "StringType"
        fieldArr.append([field_name, field_type])

    fieldArr.append(['ISOdate', 'DateType'])

    schema = [StructField(field_name, globals()[field_type](), True) for field_name,field_type  in fieldArr]

    kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})

    def mapFunc(x):
        temp = json.loads(x[1])
        res = []
        for field_name, field_type in fieldArr:
            value = temp.get(field_name, '')
            if field_name == 'ISOdate':
                value = datetime.utcfromtimestamp(temp['time'])
            res.append(value)
        return res

    lines = kvs.map(mapFunc)
    lines.pprint()

    def storeMongo(rdd):
        df = ctx.createDataFrame(rdd, schema=StructType(schema))
        df.write.format("com.mongodb.spark.sql").mode("append").options(uri=cfg.mongodb.get("host"), database=database, collection=collection).save()

    lines.foreachRDD(storeMongo)


    ssc.start()
    ssc.awaitTermination()