from kafka import KafkaProducer
import os
import boto
import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql import functions
from pyspark.sql.types import *
from pyspark.sql.functions import *


def handler(message):
    producer = KafkaProducer(bootstrap_servers = 'Kafka-DNS:9092')
    producer.send('data', str(message))
    producer.flush()
    print message


def main():

    aws_access_key = os.getenv('AWS_ACCESS_KEY_ID', 'AWS_ACCESS_KEY_ID')
    aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY', 'AWS_SECRET_ACCESS_KEY')

    spark = SparkSession.builder.appName("TRAFFIC").config("spark.executor.cores", "6").config("spark.executor.memory", "6g").getOrCreate()
    sc = spark.sparkContext
    sqlContext = SQLContext(sc)

    raw_data = sc.textFile("s3a://insighttraffic/dot_traffic_2015.txt")
    header = raw_data.first()
    records = raw_data.filter(lambda line: line != header).map(lambda x: x.split(","))
    records.cache()

    records.foreach(lambda line: handler(line))

    return

if __name__ == '__main__':
    main()
