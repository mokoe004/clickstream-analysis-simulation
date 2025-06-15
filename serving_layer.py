from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, col, current_timestamp, expr
from datetime import datetime, timedelta


def merge_batch_and_stream_tables(
    spark: SparkSession,
    keyspace: str = "clickstream",
    lookback_days: int = 2
):
    # Konfiguration aller kombinierten Tabellen
    # Format: {base_table_name: [list_of_metric_columns]}
    table_config = {
        "time_agg": ["count"],
        "campaign_events": ["event_count"],
        "campaign_actions": ["add_to_cart", "purchases"],
        "product_views": ["product_views"],
        "product_actions": ["views", "add_to_cart"],  # rate nicht kombinieren
        "agg_duration": ["avg_duration"],  # Sonderfall → hier wird Durchschnitt verwendet
        "product_purchases": ["purchases"],
        "product_cart_additions": ["cart_adds"],
        "website_views": ["views"],
        "device_distribution": ["views"],
    }

    # Zeitgrenze berechnen
    min_timestamp = (datetime.utcnow() - timedelta(days=lookback_days)).strftime("%Y-%m-%d %H:%M:%S")

    for base_name, metric_cols in table_config.items():
        batch_table = f"{base_name}_batch"
        stream_table = f"{base_name}"
        combined_table = f"{base_name}_combined"

        print(f"🔄 Kombiniere: {batch_table} + {stream_table} → {combined_table}")

        # Versuche beide Tabellen zu laden
        try:
            df_batch = spark.read \
                .format("org.apache.spark.sql.cassandra") \
                .options(table=batch_table, keyspace=keyspace) \
                .load() \
                .filter(col("window_start") >= min_timestamp)

            df_stream = spark.read \
                .format("org.apache.spark.sql.cassandra") \
                .options(table=stream_table, keyspace=keyspace) \
                .load() \
                .filter(col("window_start") >= min_timestamp)
        except Exception as e:
            print(f"⚠️  Fehler beim Laden: {e}")
            continue

        # Union beider Quellen
        df = df_batch.unionByName(df_stream)

        # Schlüsselspalten herausfinden
        key_cols = [c for c in df.columns if c not in metric_cols]

        # Sonderfall: avg_duration → kein SUM sondern AVG (optional!)
        if metric_cols == ["avg_duration"]:
            agg_expr = {col_name: "avg" for col_name in metric_cols}
        else:
            agg_expr = {col_name: "sum" for col_name in metric_cols}

        # Aggregieren
        df_combined = df.groupBy(*key_cols).agg(
            *[getattr(sum if func == "sum" else expr(func), func)(col(c)).alias(c) for c, func in agg_expr.items()]
        )

        # Ergebnis speichern
        try:
            df_combined.write \
                .format("org.apache.spark.sql.cassandra") \
                .mode("overwrite") \
                .options(table=combined_table, keyspace=keyspace) \
                .save()
            print(f"✅ Gespeichert: {combined_table}")
        except Exception as e:
            print(f"❌ Fehler beim Speichern von '{combined_table}': {e}")
