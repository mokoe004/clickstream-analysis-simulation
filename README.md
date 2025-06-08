# Simulation of clickstreams from E-Commerce shop

## Installation

 - spark-3.5.6-bin-hadoop3-scala2.13.tgz [https://spark.apache.org/downloads.html]
 - openjdk-17-jdk 

### Run Kafka
```bash 
  docker run -d --name=kafka -p 9092:9092 apache/kafka 
```
```bash 
  docker exec -ti kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server :9092 --topic clickstream
```
Test if Kafka is running (set time.sleep in kafka_test.py for slower producer)
```bash 
  docker exec -ti kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server :9092 --topic clickstream --from-beginning
```

Run Spark
```bash 
  spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.6 \
  spark_test.py 
```
