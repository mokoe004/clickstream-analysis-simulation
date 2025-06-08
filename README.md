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
  spark_processor.py 
```

Spark Docker image
```bash 
  docker run -d --name=spark -p 4040:4040 spark:3.5.6-scala2.12-java17-python3-ubuntu
```

### Run with docker compose (experimental)
This should start all necessary containers and scripts. You will get lots
of not understandable logs. When spark is running properly this means your env ist working.
```bash 
  docker compose up --build
```