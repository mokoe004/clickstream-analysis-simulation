from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_timestamp, window
from pyspark.sql.types import StructType, StringType, DoubleType

spark = (SparkSession.builder
         .appName("ClickstreamAnalytics")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

schema = StructType() \
   .add("event_id", StringType()) \
   .add("timestamp", StringType()) \
   .add("user_id", StringType()) \
   .add("session_id", StringType()) \
   .add("page", StringType()) \
   .add("action", StringType())
   # … baue den vollständigen Event-Schema nach …

raw = (spark.readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", "localhost:9092")
          .option("subscribe", "clickstream")
          .load()
          .selectExpr("CAST(value AS STRING) as json_str")
          .select(from_json("json_str", schema).alias("data"))
          .select("data.*"))
# .option("startingOffsets", "earliest")

# Beispiel: Pageviews pro Minute
agg = (raw
       .withColumn("ts", to_timestamp("timestamp"))
       .groupBy(window("ts", "1 minute"), "page")
       .count())

query = (agg.writeStream
            .outputMode("update")
            .format("console")   # für Test: in die Konsole
            .start())

query.awaitTermination()
