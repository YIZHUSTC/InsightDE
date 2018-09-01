from pyspark import sql
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
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.regression import LinearRegressionWithSGD
from pyspark.mllib.regression import LinearRegressionModel
import ast



def extract_features(record, hour, category_len, mappings):
    cat_vec = np.zeros(category_len)
    i = 0
    step = 0
    for field in record[1:11]:
        m = mappings[i]
        idx = m[field]
        cat_vec[idx + step] = 1
        i = i + 1
        step = step + len(m)
    num_vec1 = np.array([np.log(float(field.strip("\"")) + 0.01) for field in record[13 : hour + 13]])  # log transformed
    num_vec2 = np.array([np.log(float(field.strip("\"")) + 0.01) for field in record[hour + 14 : 37]])  # skip current time
    return np.concatenate((cat_vec, num_vec1, num_vec2))



def update(rdd, models, mapping):

    connection = psycopg2.connect(host = 'postgres-ip-address', database = 'postgres', user = 'postgres', password = 'postgres')
    cursor = connection.cursor()

    batch = rdd.collect()
    for record in batch:

        print record

        hour = time.localtime(time.time()).tm_hour
        p = LabeledPoint(extract_label(record, hour + 13), extract_features(record, hour, category_len, mapping))
        predicted = int(np.exp(models[hour].predict(p.features)))

        direction = str(record[4]).strip("\"")
        fips_state_code = str(record[5]).strip("\"")
        station_id = str(record[12]).strip("\"")
        sid = fips_state_code + "_" + station_id + "_" + direction
        roadtype = str(record[7]).strip("\"")
        volume = int(str(record[hour + 13]).strip("\""))
        np.random.seed(0)
        simulatedVolume = int(max(np.random.normal(volume, volume * 0.5, 1)[0],0))

        cursor.execute('SELECT * FROM traffic WHERE id = %s;', (sid,))
        rows = cursor.fetchall()
        for row in rows:
            location = row[1]
            latitude = row[2]
            longitude = row[3]
            highway = row[6]
            historical = row[7 + time.localtime(time.time()).tm_hour]
            level = "High" if (volume > historical * 2) else ("Low" if (volume < historical * 0.5) else "Medium")
            cursor.execute('INSERT INTO realtimetraffic ("sid", "location", "latitude", "longitude", "direction", "lanes", "roadtype", "highway",\
             "current", "historical", "predicted", "level", "geom") VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,\
             ST_SetSRID(ST_MakePoint(%s, %s), 4326)) ON CONFLICT (id) DO UPDATE SET current = %s;', (sid, location, latitude, longitude, direction,\
                lanes, roadtype, highway, simulatedVolume, historical, predicted, level, latitude, longitude, simulatedVolume,))


    connection.commit()
    connection.close()


def main():


    spark = SparkSession.builder.appName("TRAFFIC").config("spark.executor.cores", "4").config("spark.executor.memory", "4g").getOrCreate()
    sc = spark.sparkContext
    mapping = sc.textFile("s3a://insighttraffic/ML_model/mappings").collect()[0]
    mapping = ast.literal_eval(str(mapping))

    models=[]
    for hour in range(0, 24):
        model = LinearRegressionModel.load(sc, "s3a://insighttraffic/ML_model/linear_model_log_"+str(hour))
        models.append(model)

    category_len = 154

    sqlContext = sql.SQLContext(sc)


    hadoop_conf=sc._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3n.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3n.awsAccessKeyId", 'awsAccessKeyId')
    hadoop_conf.set("fs.s3n.awsSecretAccessKey", 'awsSecretAccessKey')

    # set microbatch interval as 10 seconds, this can be customized according to the project
    ssc = StreamingContext(sc,10)
    # directly receive the data under a certain topic
    kafkaStream = KafkaUtils.createDirectStream(ssc, ['data'], {"metadata.broker.list": 'Kafka-DNS:9092'})


    connection = psycopg2.connect(host = 'postgres-ip-address', database = 'postgres', user = 'postgres', password = 'postgres')
    cursor = connection.cursor()
    cursor.execute('CREATE TABLE IF NOT EXISTS realtimetraffic (sid text, location text, latitude double precision, longitude double precision,\
        direction text, lanes integer, roadtype text, highway text, current integer, historical double precision, level text, PRIMARY KEY (id));')
    cursor.execute('SELECT AddGeometryColumn (%s,%s,%s,4326,%s,2);', (public,realtimetraffic,geom,POINT,))


    #The inbound stream is a DStream
    dstream = kafkaStream.map(lambda (key, value): json.loads(value))
    dstream.foreachRDD(lambda rdd: update(rdd, models, mapping))
))

    ssc.start()
    ssc.awaitTermination()

    return


if __name__ == '__main__':

    main()
