import os
import boto
import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql import functions
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pandas as pd
import csv
import pyspark.sql.functions as F
from pyspark.sql import functions as sf
from collections import OrderedDict


def myConcat(*cols):
    return F.concat(*[F.coalesce(c, F.lit("*")) for c in cols])



def main():

    spark = SparkSession.builder \
            .appName("TRAFFIC") \
            .config("spark.executor.cores", "4") \
            .config("spark.executor.memory", "4gb") \
            .getOrCreate()

    sc=spark.sparkContext
    sqlContext = SQLContext(sc)

    hadoop_conf=sc._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3n.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3n.awsAccessKeyId", 'awsAccessKeyId')
    hadoop_conf.set("fs.s3n.awsSecretAccessKey", 'awsSecretAccessKey')
    
    trafficdata=sqlContext.read.csv("s3a://insighttraffic/dot_traffic_2015.txt", header="true")
    trafficdata.createOrReplaceTempView("trafficdf")    
    trafficdf = spark.sql("SELECT fips_state_code, station_id, direction_of_travel_name, \
    	traffic_volume_counted_after_0000_to_0100,traffic_volume_counted_after_0100_to_0200, \
    	traffic_volume_counted_after_0200_to_0300,traffic_volume_counted_after_0300_to_0400,traffic_volume_counted_after_0400_to_0500,\
    	traffic_volume_counted_after_0500_to_0600,traffic_volume_counted_after_0600_to_0700,traffic_volume_counted_after_0700_to_0800,\
    	traffic_volume_counted_after_0800_to_0900,traffic_volume_counted_after_0900_to_1000,traffic_volume_counted_after_1000_to_1100,\
    	traffic_volume_counted_after_1100_to_1200,traffic_volume_counted_after_1200_to_1300,traffic_volume_counted_after_1300_to_1400,\
    	traffic_volume_counted_after_1400_to_1500,traffic_volume_counted_after_1500_to_1600,traffic_volume_counted_after_1600_to_1700,\
    	traffic_volume_counted_after_1700_to_1800,traffic_volume_counted_after_1800_to_1900,traffic_volume_counted_after_1900_to_2000,\
    	traffic_volume_counted_after_2000_to_2100,traffic_volume_counted_after_2100_to_2200,traffic_volume_counted_after_2200_to_2300,\
    	traffic_volume_counted_after_2300_to_2400 FROM trafficdf")

    trafficdf = trafficdf.withColumn('id',sf.concat(sf.col('fips_state_code'),sf.lit('_'),sf.col('station_id'),sf.lit('_'),\
        sf.col('direction_of_travel_name')))
    trafficdf = trafficdf.select(trafficdf.columns[3:])

    
    stationdata=sqlContext.read.csv("s3a://insighttraffic/dot_traffic_stations_2015.csv", header="true")
    stationdata.createOrReplaceTempView("stationdf")
    stationdf = spark.sql("SELECT fips_state_code, station_id, direction_of_travel_name, station_location, latitude, longitude,\
    	lane_of_travel, functional_classification_name, national_highway_system FROM stationdf")
    stationdf = stationdf.filter((stationdf.latitude != 0) & (stationdf.longitude != 0))
    stationdf = stationdf.withColumn('id',sf.concat(sf.col('fips_state_code'),sf.lit('_'),sf.col('station_id'),sf.lit('_'),\
        sf.col('direction_of_travel_name')))
    stationdf = stationdf.select(stationdf.columns[3:])
    stationTraffic = stationdf.join(trafficdf, 'id')

    stationTraffic=stationTraffic.select(stationTraffic.id,\
        stationTraffic.station_location.alias("location"), \
    	stationTraffic.latitude.cast("double"), stationTraffic.longitude.cast("double").alias("longitudeW"), \
    	stationTraffic.lane_of_travel.alias("lanes").cast("int"), stationTraffic.functional_classification_name.alias("type"), \
    	stationTraffic.national_highway_system.alias("highway"), \
    	stationTraffic.traffic_volume_counted_after_0000_to_0100.cast("int").alias("volume00"),\
    	stationTraffic.traffic_volume_counted_after_0100_to_0200.cast("int").alias("volume01"),\
    	stationTraffic.traffic_volume_counted_after_0200_to_0300.cast("int").alias("volume02"),\
    	stationTraffic.traffic_volume_counted_after_0300_to_0400.cast("int").alias("volume03"),\
    	stationTraffic.traffic_volume_counted_after_0400_to_0500.cast("int").alias("volume04"),\
    	stationTraffic.traffic_volume_counted_after_0500_to_0600.cast("int").alias("volume05"),\
    	stationTraffic.traffic_volume_counted_after_0600_to_0700.cast("int").alias("volume06"),\
    	stationTraffic.traffic_volume_counted_after_0700_to_0800.cast("int").alias("volume07"),\
    	stationTraffic.traffic_volume_counted_after_0800_to_0900.cast("int").alias("volume08"),\
    	stationTraffic.traffic_volume_counted_after_0900_to_1000.cast("int").alias("volume09"),\
    	stationTraffic.traffic_volume_counted_after_1000_to_1100.cast("int").alias("volume10"),\
    	stationTraffic.traffic_volume_counted_after_1100_to_1200.cast("int").alias("volume11"),\
    	stationTraffic.traffic_volume_counted_after_1200_to_1300.cast("int").alias("volume12"),\
    	stationTraffic.traffic_volume_counted_after_1300_to_1400.cast("int").alias("volume13"),\
    	stationTraffic.traffic_volume_counted_after_1400_to_1500.cast("int").alias("volume14"),\
    	stationTraffic.traffic_volume_counted_after_1500_to_1600.cast("int").alias("volume15"),\
    	stationTraffic.traffic_volume_counted_after_1600_to_1700.cast("int").alias("volume16"),\
    	stationTraffic.traffic_volume_counted_after_1700_to_1800.cast("int").alias("volume17"),\
    	stationTraffic.traffic_volume_counted_after_1800_to_1900.cast("int").alias("volume18"),\
    	stationTraffic.traffic_volume_counted_after_1900_to_2000.cast("int").alias("volume19"),\
    	stationTraffic.traffic_volume_counted_after_2000_to_2100.cast("int").alias("volume20"),\
    	stationTraffic.traffic_volume_counted_after_2100_to_2200.cast("int").alias("volume21"),\
    	stationTraffic.traffic_volume_counted_after_2200_to_2300.cast("int").alias("volume22"),\
    	stationTraffic.traffic_volume_counted_after_2300_to_2400.cast("int").alias("volume23"))

    trafficAvg = stationTraffic.groupby(['id', 'location', 'latitude', 'longitudeW', 'lanes',\
    	'type', 'highway']).agg({"volume00":"avg","volume01":"avg", "volume02":"avg","volume03":"avg","volume04":"avg",\
        "volume05":"avg","volume06":"avg","volume07":"avg", "volume08":"avg","volume09":"avg", "volume10":"avg","volume11":"avg",\
        "volume12":"avg","volume13":"avg", "volume14":"avg","volume15":"avg","volume16":"avg","volume17":"avg","volume18":"avg",\
        "volume19":"avg","volume20":"avg","volume21":"avg", "volume22":"avg","volume23":"avg"})
        
    trafficAvg=trafficAvg.withColumn('longitude',0.0-sf.col("longitudeW"))
    trafficAvg=trafficAvg.withColumnRenamed('avg(volume00)', 'avgvolume00')
    trafficAvg=trafficAvg.withColumnRenamed('avg(volume01)', 'avgvolume01')
    trafficAvg=trafficAvg.withColumnRenamed('avg(volume02)', 'avgvolume02')
    trafficAvg=trafficAvg.withColumnRenamed('avg(volume03)', 'avgvolume03')
    trafficAvg=trafficAvg.withColumnRenamed('avg(volume04)', 'avgvolume04')
    trafficAvg=trafficAvg.withColumnRenamed('avg(volume05)', 'avgvolume05')
    trafficAvg=trafficAvg.withColumnRenamed('avg(volume06)', 'avgvolume06')
    trafficAvg=trafficAvg.withColumnRenamed('avg(volume07)', 'avgvolume07')
    trafficAvg=trafficAvg.withColumnRenamed('avg(volume08)', 'avgvolume08')
    trafficAvg=trafficAvg.withColumnRenamed('avg(volume09)', 'avgvolume09')
    trafficAvg=trafficAvg.withColumnRenamed('avg(volume10)', 'avgvolume10')
    trafficAvg=trafficAvg.withColumnRenamed('avg(volume11)', 'avgvolume11')
    trafficAvg=trafficAvg.withColumnRenamed('avg(volume12)', 'avgvolume12')
    trafficAvg=trafficAvg.withColumnRenamed('avg(volume13)', 'avgvolume13')
    trafficAvg=trafficAvg.withColumnRenamed('avg(volume14)', 'avgvolume14')
    trafficAvg=trafficAvg.withColumnRenamed('avg(volume15)', 'avgvolume15')
    trafficAvg=trafficAvg.withColumnRenamed('avg(volume16)', 'avgvolume16')
    trafficAvg=trafficAvg.withColumnRenamed('avg(volume17)', 'avgvolume17')
    trafficAvg=trafficAvg.withColumnRenamed('avg(volume18)', 'avgvolume18')
    trafficAvg=trafficAvg.withColumnRenamed('avg(volume19)', 'avgvolume19')
    trafficAvg=trafficAvg.withColumnRenamed('avg(volume20)', 'avgvolume20')
    trafficAvg=trafficAvg.withColumnRenamed('avg(volume21)', 'avgvolume21')
    trafficAvg=trafficAvg.withColumnRenamed('avg(volume22)', 'avgvolume22')
    trafficAvg=trafficAvg.withColumnRenamed('avg(volume23)', 'avgvolume23')

    trafficAvg=trafficAvg.select('id', 'location', 'latitude', 'longitudeW', 'lanes','type', 'highway',\
        'avgvolume00','avgvolume01','avgvolume02','avgvolume03','avgvolume04','avgvolume05','avgvolume06','avgvolume07',\
        'avgvolume08','avgvolume09','avgvolume10','avgvolume11','avgvolume12','avgvolume13','avgvolume14','avgvolume15',\
        'avgvolume16','avgvolume17','avgvolume18','avgvolume19','avgvolume20','avgvolume21','avgvolume22','avgvolume23')

    trafficAvg.write.jdbc(url = "jdbc:postgresql://postgres-ip-address:5432/postgres",table = "traffic",mode="overwrite",\
        properties={"password":"postgres","user":"postgres"})


    sc.stop()


if __name__ == '__main__':
    main()
