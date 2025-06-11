from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import to_timestamp, window, when, sum, col, avg


def build_batch_aggregations(df: DataFrame):
    raw = df.withColumn("ts", to_timestamp("timestamp"))

    aggregations = []

    # 1. time_agg
    time_agg = (raw
        .groupBy(window("ts", "1 minute"), "page")
        .count()
        .withColumn("window_start", col("window").start)
        .withColumn("window_end", col("window").end)
        .drop("window"))
    aggregations.append((time_agg, "time_agg_batch", "cassandra"))

    # 2. campaign_events
    campaign_events = (raw
        .groupBy(window("ts", "5 minutes"), "utm_campaign", "utm_source")
        .count()
        .withColumnRenamed("count", "event_count")
        .withColumn("window_start", col("window").start)
        .withColumn("window_end", col("window").end)
        .drop("window"))
    aggregations.append((campaign_events, "campaign_events_batch", "cassandra"))

    # 3. campaign_actions
    campaign_actions = (raw
        .withColumn("is_add_to_cart", when(col("action") == "add_to_cart", 1).otherwise(0))
        .withColumn("is_purchase", when(col("action") == "purchase", 1).otherwise(0))
        .groupBy(window("ts", "1 hour"), "utm_campaign")
        .agg(
            sum("is_add_to_cart").alias("add_to_cart"),
            sum("is_purchase").alias("purchases")
        )
        .withColumn("window_start", col("window").start)
        .withColumn("window_end", col("window").end)
        .drop("window"))
    aggregations.append((campaign_actions, "campaign_actions_batch", "cassandra"))

    # 4. product_views
    product_views = (raw
        .filter(col("page") == "product_detail")
        .groupBy(window("ts", "1 hour"), "product_id")
        .count()
        .withColumnRenamed("count", "product_views")
        .withColumn("window_start", col("window").start)
        .withColumn("window_end", col("window").end)
        .drop("window"))
    aggregations.append((product_views, "product_views_batch", "cassandra"))

    # 5. product_actions
    product_actions = (raw
        .filter(col("product_id").isNotNull())
        .withColumn("is_view", when(col("action") == "view", 1).otherwise(0))
        .withColumn("is_add", when(col("action") == "add_to_cart", 1).otherwise(0))
        .groupBy(window("ts", "1 hour"), "product_id")
        .agg(
            sum("is_view").alias("views"),
            sum("is_add").alias("add_to_cart")
        )
        .withColumn("add_to_cart_rate", col("add_to_cart") / col("views"))
        .withColumn("window_start", col("window").start)
        .withColumn("window_end", col("window").end)
        .drop("window"))
    aggregations.append((product_actions, "product_actions_batch", "cassandra"))

    # 6. agg_duration
    agg_duration = (raw
        .groupBy(window("ts", "10 minutes"), "page")
        .agg(avg("page_duration").alias("avg_duration"))
        .withColumn("window_start", col("window").start)
        .withColumn("window_end", col("window").end)
        .drop("window"))
    aggregations.append((agg_duration, "agg_duration_batch", "cassandra"))

    # 7. product_purchases
    product_purchases = (raw
        .filter((col("action") == "purchase") & col("product_id").isNotNull())
        .groupBy(window("ts", "1 hour"), "product_id")
        .count()
        .withColumnRenamed("count", "purchases")
        .withColumn("window_start", col("window").start)
        .withColumn("window_end", col("window").end)
        .drop("window"))
    aggregations.append((product_purchases, "product_purchases_batch", "cassandra"))

    # 8. product_cart_additions
    product_cart = (raw
        .filter((col("action") == "add_to_cart") & col("product_id").isNotNull())
        .groupBy(window("ts", "1 hour"), "product_id")
        .count()
        .withColumnRenamed("count", "cart_adds")
        .withColumn("window_start", col("window").start)
        .withColumn("window_end", col("window").end)
        .drop("window"))
    aggregations.append((product_cart, "product_cart_additions_batch", "cassandra"))

    # 9. website_views
    website_views = (raw
        .groupBy(window("ts", "1 hour"))
        .count()
        .withColumnRenamed("count", "views")
        .withColumn("window_start", col("window").start)
        .withColumn("window_end", col("window").end)
        .drop("window"))
    aggregations.append((website_views, "website_views_batch", "cassandra"))

    # 10. device_distribution
    device_distribution = (raw
        .groupBy(window("ts", "1 hour"), "device_type")
        .count()
        .withColumnRenamed("count", "views")
        .withColumn("window_start", col("window").start)
        .withColumn("window_end", col("window").end)
        .drop("window"))
    aggregations.append((device_distribution, "device_distribution_batch", "cassandra"))

    return aggregations
