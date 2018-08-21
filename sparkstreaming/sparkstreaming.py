from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import Row, SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col
from pyspark import sql
import json, math, datetime
import csv
import io
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import Row, SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col
import json, math, datetime
from kafka.consumer import SimpleConsumer
import psycopg2
from operator import add
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from cassandra import ConsistencyLevel
import time


def update(rdd):

    connection = psycopg2.connect(host = '52.35.209.191', database = 'postgres', user = 'postgres', password = 'postgres')
    cursor = connection.cursor()

    list = rdd.collect()
    for element in list:
        id = element["id"]
        volume = element["volume"]
        cursor.execute('SELECT * FROM traffic WHERE id = %s;', (id,))
        rows = cursor.fetchall()
        for row in rows:
            location = row[1]
            latitude = row[2]
            longitude = row[3]
            lanes = row[4]
            type = row[5]
            highway = row[6]
            historical = row[7 + time.localtime(time.time()).tm_hour]
            level = "High" if (volume > historical * 2) else ("Low" if (volume < historical * 0.5) else "Medium")
            cursor.execute('INSERT INTO realtime("id","location","latitude","longitude","lanes","type","highway","current","historical","level","geom") \
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, ST_SetSRID(ST_MakePoint(%s,%s), 4326)) ON CONFLICT (id) DO UPDATE SET current = %s;', \
                (id,location,latitude,longitude,lanes,type,highway,volume,historical,level,latitude,longitude,volume,))
    connection.commit()
    connection.close()


def main():
    
    sc = SparkContext(appName="PythonSparkStreamingKafka")
    sc.setLogLevel("WARN")
    sqlContext = sql.SQLContext(sc)

    ssc = StreamingContext(sc,5)
    kafkaStream = KafkaUtils.createDirectStream(ssc, ['data'], {"metadata.broker.list": 'ec2-52-13-250-149.us-west-2.compute.amazonaws.com:9092'})


    dstream = kafkaStream.map(lambda (key, value): json.loads(value))
    dstream.foreachRDD(lambda rdd: update(rdd))

    ssc.start()
    ssc.awaitTermination()

    return


if __name__ == '__main__':

    main()
