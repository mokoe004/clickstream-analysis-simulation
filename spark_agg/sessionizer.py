from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, min, max, unix_timestamp, collect_list, current_timestamp, expr,
    avg, count, array_contains, concat_ws, to_timestamp
)


def compute_session_aggregations(df: DataFrame):
    """
    Computes multiple marketing-relevant session-level aggregations.
    Returns a list of tuples: (DataFrame, table_name, output_target)
    Output target is typically 'cassandra' or 'console'.
    """

    # 1. Base session DataFrame: one row per session with duration and visited content
    df_with_ts = df.withColumn("ts", to_timestamp("timestamp")).withWatermark("ts", "30 minutes")

    session_df = (
        df_with_ts
        .groupBy("session_id")
        .agg(
            min("ts").alias("session_start"),
            max("ts").alias("session_end"),
            collect_list("page").alias("visited_pages"),
            collect_list("action").alias("actions")
        )
        .withColumn("session_duration_seconds",
                    unix_timestamp("session_end") - unix_timestamp("session_start"))
        .withColumn("ts", col("session_end"))  # für downstream filter
    )

    session_df.printSchema()
    session_df.explain(True)

    aggregations = []

    # 2. Active sessions in the last 15 minutes – live traffic indicator
    active_sessions = (
        session_df
        .filter(col("session_end") >= current_timestamp() - expr("INTERVAL 15 MINUTES"))
        .selectExpr("count(*) as active_sessions")
    )

    aggregations.append((active_sessions, "active_sessions", "cassandra"))

    # 3. Average session duration – engagement level indicator
    session_duration_avg = (
        session_df
        .agg(avg("session_duration_seconds").alias("avg_session_duration"))
    )
    aggregations.append((session_duration_avg, "avg_session_duration", "cassandra"))

    # 4. Most common user flows (visited page sequences) – useful for UX/funnel optimization
    funnel_paths = (
        session_df
        .withColumn("path", concat_ws(" > ", "visited_pages"))
        .groupBy("path")
        .count()
        #.orderBy(col("count").desc())
    )
    aggregations.append((funnel_paths, "funnel_paths", "cassandra"))

    # 5. Session conversion rate – sessions that included a 'purchase' action
    conversions = (
        session_df
        .withColumn("is_conversion", array_contains("actions", "purchase").cast("int"))
        .agg(avg("is_conversion").alias("session_conversion_rate"))
    )
    aggregations.append((conversions, "session_conversion_rate", "cassandra"))

    # 6. Sessions per campaign and source – good for campaign ROI tracking
    #session_campaigns = (
    #    df
    #    .select("session_id", "utm_campaign", "utm_source", "timestamp")
    #    .dropDuplicates(["session_id"])
    #    .join(session_df, on="session_id")
    #    .groupBy("utm_campaign", "utm_source")
    #    .agg(
    #        count("*").alias("sessions"),
    #        avg("session_duration_seconds").alias("avg_session_duration"),
    #    )
    #)
    #aggregations.append((session_campaigns, "session_campaigns", "cassandra"))

    return aggregations
