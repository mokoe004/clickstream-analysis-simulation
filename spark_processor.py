from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_timestamp, window, when, sum, col, avg
from pyspark.sql.types import StringType, DoubleType, IntegerType, BooleanType, StructType

from spark_agg.sessionizer import compute_sessions


class ClickstreamAnalyticsJob:
    def __init__(self, kafka_bootstrap_servers="localhost:9092", kafka_topic="clickstream"):
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.kafka_topic = kafka_topic
        self.spark = self._init_spark()
        self.schema = self._define_schema()
        self.raw_stream = self._read_stream()
        self.aggregations = []

    def _init_spark(self):
        spark = (SparkSession.builder
                 .appName("ClickstreamAnalytics")
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

    def build_aggregations(self):
        raw = self.raw_stream

        time_agg = (raw
            .withColumn("ts", to_timestamp("timestamp"))
            .groupBy(window("ts", "1 minute"), "page")
            .count())
        self.aggregations.append((time_agg, "time_agg"))

        agg_campaign = (
            raw
            .withColumn("ts", to_timestamp("timestamp"))
            .groupBy(window("ts", "5 minutes"), "utm_campaign", "utm_source")
            .count()
            .withColumnRenamed("count", "event_count")
        )
        self.aggregations.append((agg_campaign, "campaign_events"))

        agg_campaign_actions = (
            raw
            .withColumn("ts", to_timestamp("timestamp"))
            .withColumn("is_add_to_cart", when(col("action") == "add_to_cart", 1).otherwise(0))
            .withColumn("is_purchase", when(col("action") == "purchase", 1).otherwise(0))
            .groupBy(window("ts", "1 hour"), "utm_campaign")
            .agg(
                sum("is_add_to_cart").alias("add_to_cart"),
                sum("is_purchase").alias("purchases")
            )
        )
        self.aggregations.append((agg_campaign_actions, "campaign_actions"))

        agg_product_views = (
            raw
            .withColumn("ts", to_timestamp("timestamp"))
            .filter(col("page") == "product_detail")
            .groupBy(window("ts", "1 hour"), "product_id")
            .count()
            .withColumnRenamed("count", "product_views")
        )
        self.aggregations.append((agg_product_views, "product_views"))

        product_actions = (
            raw
            .withColumn("ts", to_timestamp("timestamp"))
            .filter(col("product_id").isNotNull())
            .withColumn("is_view", when(col("action") == "view", 1).otherwise(0))
            .withColumn("is_add", when(col("action") == "add_to_cart", 1).otherwise(0))
            .groupBy(window("ts", "1 hour"), "product_id")
            .agg(
                sum("is_view").alias("views"),
                sum("is_add").alias("add_to_cart")
            )
            .withColumn("add_to_cart_rate", col("add_to_cart") / col("views"))
        )
        self.aggregations.append((product_actions, "product_actions"))

        agg_duration = (
            raw
            .withColumn("ts", to_timestamp("timestamp"))
            .groupBy(window("ts", "10 minutes"), "page")
            .agg(avg("page_duration").alias("avg_duration"))
        )
        self.aggregations.append((agg_duration, "agg_duration"))

    def start_streams(self):
        query_handles = []

        for df, name in self.aggregations:
            query = (df.writeStream
                     .outputMode("update")
                     .format("console")
                     .option("truncate", False)
                     .queryName(name)
                     .start())
            query_handles.append(query)

        # Sessionizer
        session_df = compute_sessions(self.raw_stream)
        session_query = (session_df.writeStream
                         .outputMode("complete")
                         .format("console")
                         .start())
        query_handles.append(session_query)

        self.spark.streams.awaitAnyTermination()

    def run(self):
        self.build_aggregations()
        self.start_streams()


if __name__ == "__main__":
    job = ClickstreamAnalyticsJob()
    job.run()
