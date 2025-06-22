# Simulation of Clickstreams from an E-Commerce Shop

## Installation

- Download Apache Spark 3.5.6 (Scala 2.13, Hadoop 3): https://spark.apache.org/downloads.html
- Install openjdk-17-jdk

---

## Run Kafka manually

Consume and test output. Jump into kafka container and execute:

    kafka-console-consumer --bootstrap-server kafka:9092 --topic clickstream --from-beginning

Kafka commands:
```bash
  # DOCKER COMMANDS
  
  # docker run -d --name=kafka -p 9092:9092 apache/kafka
  
  # docker exec -ti kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server :9092 --topic clickstream
  
  # docker exec -ti kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server :9092 --topic clickstream --from-beginning
```
---

## Cassandra Commands
```bash
  cqlsh
```
```sql
  SELECT * FROM agg_duration LIMIT 5;
  SELECT * FROM campaign_actions LIMIT 5;
  SELECT * FROM campaign_events LIMIT 5;
  SELECT * FROM device_distribution LIMIT 5;
  SELECT * FROM product_actions LIMIT 5;
  SELECT * FROM product_cart_additions LIMIT 5;
  SELECT * FROM product_purchases LIMIT 5;
  SELECT * FROM product_views LIMIT 5;
  SELECT * FROM time_agg LIMIT 5;
  SELECT * FROM website_views LIMIT 5;
```
```sql
  TRUNCATE agg_duration;
  TRUNCATE campaign_actions;
  TRUNCATE campaign_events;
  TRUNCATE device_distribution;
  TRUNCATE product_actions;
  TRUNCATE product_cart_additions;
  TRUNCATE product_purchases;
  TRUNCATE product_views;
  TRUNCATE time_agg;
  TRUNCATE website_views;
```
```sql
  TRUNCATE agg_duration_batch;
  TRUNCATE campaign_actions_batch;
  TRUNCATE campaign_events_batch;
  TRUNCATE device_distribution_batch;
  TRUNCATE product_actions_batch;
  TRUNCATE product_cart_additions_batch;
  TRUNCATE product_purchases_batch;
  TRUNCATE product_views_batch;
  TRUNCATE time_agg_batch;
  TRUNCATE website_views_batch;
```
---
## Run Spark manually

    spark-submit \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.6 \
      spark_processor.py

Optional: run Spark in a container (monitor UI at port 4040):

    docker run -d --name=spark -p 4040:4040 spark:3.5.6-scala2.12-java17-python3-ubuntu

Important: set PROCESSING_MODE env in docker-compose.yml
---

## Run via Docker Compose (experimental)

Starts all services: Zookeeper, Kafka, Cassandra, Spark, Producer, and Dashboard.

    docker compose up --build

You’ll see a lot of logs. If Spark starts successfully, the setup works. You may need to start containers manually due
to race conditions.

---

## Run the Dashboard

Once docker compose is running:

Open http://localhost:8050 in your browser.

---

## Dev Notes

- Startup time is long: Use docker-compose selectively during development.
  
- To only test the dashboard:

      docker compose up cassandra cassandra-init dashboard

  (Assumes Cassandra already contain data)

- To fully run the system with fresh clickstreams:

      docker compose up --build

  - For working with Kafka manually:

        docker compose up zookeeper kafka topic-init
        docker compose exec -ti kafka kafka-console-consumer.sh --bootstrap-server :9092 --topic clickstream --from-beginning

  - Rebuild docker compose

        docker compose down --volumes --remove-orphans
        docker compose build --no-cache  

  - Then: 
    
         docker compose build --no-cache

---

## Status

Work in progress. System components are modular and evolving.
