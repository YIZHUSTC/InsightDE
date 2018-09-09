spark-submit --master spark://ip-10-0-0-10:7077 --driver-memory 6g --executor-memory 6g --num-executors 6 --executor-cores 6 --driver-class-path ~/spark/postgresql-42.2.4.jar spark.py
spark-submit --master spark://ip-10-0-0-10:7077 --driver-memory 6g --executor-memory 6g --num-executors 6 --executor-cores 6 sparkml.py
