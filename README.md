--Tips:

1 Controller  file name must be the class name

2 Controller Class  method must be statis 


--submit Spark  exmple
./bin/spark-submit --master "local[4]" \
--jars  external/kafka-0-8-assembly/target/spark-streaming-kafka-0-8-assembly_2.11-2.2.3-SNAPSHOT.jar \
--packages org.mongodb.spark:mongo-spark-connector_2.11:2.2.3  \
--executor-memory 256m  \
--py-files  /home/litingting/apps/batch/bigdata.zip   \
 /home/litingting/apps/batch/bigdata/cashseedKafkaToMongo.py  


--install 
 sudo pip3 install pymongo (mongodb server version 3.6)
 sudo pip3 install redis


--mongodb db: 
 source:  initial data
 stats:  After processing data


--spark  python依赖包打包运行
1、打包
cd  xxxx/bigdata;
git pull
zip -r ../bigdata.zip  ./*  -x './bigdata/.git/*'

2、在.bashrc添加环境变量
export SPARK_PY_PATH=xxxx/bigdata

3、提交spark-submit 应用时
./bin/spark-submit --py-files  xxxx/../bigdata.zip  








