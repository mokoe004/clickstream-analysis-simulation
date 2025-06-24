
# 🛍️ Simulation of Clickstreams from an E-Commerce Shop

This project simulates clickstream behavior from users on an e-commerce website. It supports real-time streaming and batch processing, stores analytics results in Cassandra, and visualizes them through a dashboard.

---

## 🚀 Getting Started

### 🔧 Prerequisites

- **Apache Spark 3.5.6** (Scala 2.13, Hadoop 3): [Download here](https://spark.apache.org/downloads.html)
- **Java 17**:  
  Install via:

  ```bash
  sudo apt install openjdk-17-jdk
  ```

---

## 🐳 Run via Docker Compose

Start the complete pipeline including Zookeeper, Kafka, Cassandra, Spark, Producer, and Dashboard:

```bash
docker compose up --build
```

You’ll see a lot of logs. If Spark starts successfully, everything is working.

⚠️ **Startup issues?**
Sometimes services fail to start in the correct order. If components crash, just start them individually:

```bash
docker compose up zookeeper
docker compose up kafka
docker compose up cassandra
docker compose up cassandra-init
docker compose up stream-producer
docker compose up spark
docker compose up dashboard
```

To reset and rebuild everything cleanly:

```bash
docker compose down --volumes --remove-orphans
docker compose build --no-cache
docker compose up
```

---

## 🧪 Kafka: Manual Consumer Test

To test the stream:

```bash
docker compose exec kafka kafka-console-consumer   --bootstrap-server kafka:9092   --topic clickstream   --from-beginning
```

---

## 🧾 Cassandra: Query Interface

Enter Cassandra shell:

```bash
docker compose exec cassandra cqlsh
```

Sample queries:

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

To truncate (clear) tables:

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

Also for `_batch` tables:

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

## 🔥 Spark Streaming / Batch Mode

The Spark job can run in two modes: **streaming** or **batch**.
The mode is controlled in the `main()` function of `spark_processor.py`.

### ✅ To switch modes:

1. Edit `spark_processor.py`:

   ```python
    if __name__ == "__main__":
        job = ClickstreamAnalyticsJob() 
        job.run("stream") # or "batch"
   ```

2. Restart the Spark container:

   ```bash
   docker compose restart spark
   ```

### 📺 Watch microbatch output

To observe processing:

```bash
docker compose logs -f spark
```

The logs will show when microbatches are triggered and when data is written to Cassandra.

---

## 📊 Run the Dashboard

Once services are running:

Open your browser at [http://localhost:8050](http://localhost:8050)

---

## 🧑‍💻 Developer Notes

* **Use only needed services during development** to reduce startup time.

### Run Dashboard Only (assuming data exists):

```bash
docker compose up cassandra cassandra-init dashboard
```

### Run Kafka manually:

```bash
docker compose up zookeeper kafka topic-init
```

Then:

```bash
docker compose exec kafka kafka-console-consumer   --bootstrap-server kafka:9092 --topic clickstream --from-beginning
```

---

## 🔍 Jump Into Container Terminals

### Kafka

```bash
docker compose exec kafka bash
```

Inside container:

```bash
kafka-console-consumer --bootstrap-server kafka:9092 --topic clickstream --from-beginning
```

### Cassandra

```bash
docker compose exec cassandra cqlsh
```

### Spark

```bash
docker compose exec spark bash
```

Check logs or run diagnostic scripts manually from `/app`.

### Producer

```bash
docker compose exec stream-producer bash
```

### Dashboard

```bash
docker compose exec dashboard bash
```

---

## 🧱 System Architecture

* **Kafka**: Message bus for clickstreams
* **Spark**: Streaming & batch processor
* **Cassandra**: Storage backend
* **Dashboard**: Visualization frontend (Dash/Plotly)
* **Producer**: Generates synthetic clickstream data
* **Batch Generator**: Optional for static CSV generation

---

## 📦 Versioning

This setup uses:

* Docker Compose spec: `3.8`
* Spark: `3.5.6`
* Cassandra: `4.1`
* Kafka: `7.6.0`

---

## 🚧 Status

🛠️ **Work in Progress**
All components are modular and still under active development.