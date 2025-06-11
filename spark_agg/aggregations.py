from pyspark.sql.functions import to_timestamp, window, when, sum, col, avg

def build_aggregations(raw_stream) -> list:
    raw = raw_stream.withColumn("ts", to_timestamp("timestamp"))
    aggregations = []
    # time_agg mit Watermark
    time_agg = (raw
                .withWatermark("ts", "10 minutes")
                .groupBy(window("ts", "1 minute"), "page")
                .count()
                .withColumn("window_start", col("window").start)
                .withColumn("window_end", col("window").end)
                .drop("window"))
    aggregations.append((time_agg, "time_agg", "cassandra"))

    # campaign_events mit Watermark
    agg_campaign = (
        raw
        .withWatermark("ts", "10 minutes")
        .groupBy(window("ts", "5 minutes"), "utm_campaign", "utm_source")
        .count()
        .withColumnRenamed("count", "event_count")
        .withColumn("window_start", col("window").start)
        .withColumn("window_end", col("window").end)
        .drop("window"))
    aggregations.append((agg_campaign, "campaign_events", "cassandra"))

    # campaign_actions mit Watermark
    agg_campaign_actions = (
        raw
        .withWatermark("ts", "10 minutes")
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
    aggregations.append((agg_campaign_actions, "campaign_actions", "cassandra"))

    # product_views mit Watermark
    agg_product_views = (
        raw
        .withWatermark("ts", "10 minutes")
        .filter(col("page") == "product_detail")
        .groupBy(window("ts", "1 hour"), "product_id")
        .count()
        .withColumnRenamed("count", "product_views")
        .withColumn("window_start", col("window").start)
        .withColumn("window_end", col("window").end)
        .drop("window"))
    aggregations.append((agg_product_views, "product_views", "cassandra"))

    # product_actions mit Watermark
    product_actions = (
        raw
        .withWatermark("ts", "10 minutes")
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
    aggregations.append((product_actions, "product_actions", "cassandra"))

    # agg_duration mit Watermark
    agg_duration = (
        raw
        .withWatermark("ts", "10 minutes")
        .groupBy(window("ts", "10 minutes"), "page")
        .agg(avg("page_duration").alias("avg_duration"))
        .withColumn("window_start", col("window").start)
        .withColumn("window_end", col("window").end)
        .drop("window"))
    aggregations.append((agg_duration, "agg_duration", "cassandra"))

    # 1. Produkt-Käufe
    agg_product_purchases = (
        raw
        .withWatermark("ts", "10 minutes")
        .filter((col("action") == "purchase") & (col("product_id").isNotNull()))
        .groupBy(window("ts", "1 hour"), "product_id")
        .count()
        .withColumnRenamed("count", "purchases")
        .withColumn("window_start", col("window").start)
        .withColumn("window_end", col("window").end)
        .drop("window")
    )
    aggregations.append((agg_product_purchases, "product_purchases", "cassandra"))

    # 2. Produkte im Warenkorb
    agg_product_cart = (
        raw
        .withWatermark("ts", "10 minutes")
        .filter((col("action") == "add_to_cart")  & (col("product_id").isNotNull()))
        .groupBy(window("ts", "1 hour"), "product_id")
        .count()
        .withColumnRenamed("count", "cart_adds")
        .withColumn("window_start", col("window").start)
        .withColumn("window_end", col("window").end)
        .drop("window")
    )
    aggregations.append((agg_product_cart, "product_cart_additions", "cassandra"))

    # 3. Website Views gesamt
    agg_website_views = (
        raw
        .withWatermark("ts", "10 minutes")
        .groupBy(window("ts", "1 hour"))
        .count()
        .withColumnRenamed("count", "views")
        .withColumn("window_start", col("window").start)
        .withColumn("window_end", col("window").end)
        .drop("window")
    )
    aggregations.append((agg_website_views, "website_views", "cassandra"))

    # 4. Geräteverteilung
    agg_device_distribution = (
        raw
        .withWatermark("ts", "10 minutes")
        .groupBy(window("ts", "1 hour"), "device_type")
        .count()
        .withColumnRenamed("count", "views")
        .withColumn("window_start", col("window").start)
        .withColumn("window_end", col("window").end)
        .drop("window")
    )
    aggregations.append((agg_device_distribution, "device_distribution", "cassandra"))

    return aggregations

    # # 5. Kampagnen-Events
    # agg_campaign_events = (
    #     raw
    #     .withWatermark("ts", "10 minutes")
    #     .filter(col("utm_campaign").isNotNull())
    #     .groupBy(window("ts", "1 hour"), "utm_campaign")
    #     .count()
    #     .withColumnRenamed("count", "event_count")
    #     .withColumn("window_start", col("window").start)
    #     .withColumn("window_end", col("window").end)
    #     .drop("window")
    # )
    # self.aggregations.append((agg_campaign_events, "campaign_events", "campaign_events"))
