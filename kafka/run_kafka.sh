/usr/local/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 3 --partitions 4 --topic data
spark-submit  --master spark://ip-10-0-0-8:7077 --driver-memory 6g --executor-memory 6g --num-executors 6 --executor-cores 6 kafkaproducer.py
