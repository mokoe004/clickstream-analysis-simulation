from pyspark.sql import DataFrame
from pyspark.sql.functions import col, min, max, unix_timestamp, collect_list

def compute_sessions(df: DataFrame) -> DataFrame:
    """
    Aggregiert Events nach session_id, um Sessions zu identifizieren
    und z. B. Sessiondauer, besuchte Seiten und Funnel-Schritte zu berechnen.
    """

    session_df = (
        df
        .withColumn("ts", col("timestamp").cast("timestamp"))
        .groupBy("session_id")
        .agg(
            min("ts").alias("session_start"),
            max("ts").alias("session_end"),
            collect_list("page").alias("visited_pages"),
            collect_list("action").alias("actions")
        )
        .withColumn("session_duration_seconds",
                    unix_timestamp("session_end") - unix_timestamp("session_start"))
    )

    return session_df
