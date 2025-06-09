import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_timestamp, window, when, sum, col, avg
from pyspark.sql.types import StringType, DoubleType, IntegerType, BooleanType, StructType

from spark_agg.sessionizer import compute_session_aggregations
from spark_agg.aggregations import build_aggregations


class ClickstreamAnalyticsJob:
    def __init__(self, kafka_bootstrap_servers="localhost:9092", kafka_topic="clickstream"):
        self.kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP", kafka_bootstrap_servers)
        self.kafka_topic = kafka_topic
        self.spark = self._init_spark()
        self.schema = self._define_schema()
        self.raw_stream = self._read_stream()
        self.aggregations = []

    def _init_spark(self):
        spark = (SparkSession.builder
                 .appName("ClickstreamAnalytics")
                 .config("spark.cassandra.connection.host", "cassandra")
                 .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false")
                 .getOrCreate())
        spark.sparkContext.setLogLevel("WARN")
        return spark

    def _define_schema(self):
        return StructType() \
            .add("event_id", StringType()) \
            .add("timestamp", StringType()) \
            .add("user_id", StringType()) \
            .add("session_id", StringType()) \
            .add("page", StringType()) \
            .add("page_url", StringType()) \
            .add("product_id", StringType()) \
            .add("category", StringType()) \
            .add("action", StringType()) \
            .add("element_id", StringType()) \
            .add("position_x", IntegerType()) \
            .add("position_y", IntegerType()) \
            .add("referrer", StringType()) \
            .add("utm_source", StringType()) \
            .add("utm_medium", StringType()) \
            .add("utm_campaign", StringType()) \
            .add("device_type", StringType()) \
            .add("os", StringType()) \
            .add("browser", StringType()) \
            .add("language", StringType()) \
            .add("ip_address", StringType()) \
            .add("geo_country", StringType()) \
            .add("geo_region", StringType()) \
            .add("geo_city", StringType()) \
            .add("page_duration", DoubleType()) \
            .add("scroll_depth_percent", IntegerType()) \
            .add("ab_test_variant", StringType()) \
            .add("is_logged_in", BooleanType())

    def _read_stream(self):
        return (self.spark.readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers)
                .option("subscribe", self.kafka_topic)
                .option("startingOffsets", "earliest")
                .load()
                .selectExpr("CAST(value AS STRING) as json_str")
                .select(from_json("json_str", self.schema).alias("data"))
                .select("data.*"))

    def _write_to_cassandra(self, df_, table_name):
        if "window" in df_.columns:
            df_ = df_ \
                .withColumn("window_start", col("window").start) \
                .withColumn("window_end", col("window").end) \
                .drop("window")

        df_.write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("append") \
            .options(table=table_name, keyspace="clickstream") \
            .save()

    def start_streams(self):
        queries = []

        for df, name, output_format in self.aggregations:
            def write_batch(df_, batch_id, table=name):
                print(f"📤 Writing {table} batch {batch_id} to Cassandra")
                self._write_to_cassandra(df_, table)

            if output_format == "cassandra":
                query = df.writeStream \
                    .foreachBatch(write_batch) \
                    .outputMode("update") \
                    .option("checkpointLocation", f"/tmp/checkpoints/{name}") \
                    .start()
            else:
                query = df.writeStream \
                    .outputMode("complete") \
                    .format("console") \
                    .start()

            queries.append(query)

        # WICHTIG: auf alle warten
        for q in queries:
            q.awaitTermination()

    def run(self):
        self.aggregations.extend(build_aggregations(self.raw_stream))
        #self.aggregations.extend(compute_session_aggregations(self.raw_stream))
        self.start_streams()


if __name__ == "__main__":
    job = ClickstreamAnalyticsJob()
    job.run()
    df = job.raw_stream  # oder ein Aggregat aus build_aggregations()
    df.printSchema()
    df.write.format("console").save()

