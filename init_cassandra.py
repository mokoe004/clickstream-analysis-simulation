
# docker run --name cassandra-db -p 9042:9042 -d cassandra:latest

# --name cassandra-db gibt dem Container einen Namen
# -p 9042:9042 mapped den Cassandra-Port (CQL) vom Container zu deinem Host
# -d läuft den Container im Hintergrund

# docker exec -it cassandra-db cqlsh (Falls man manuell was machen will)

# spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.13:3.4.0 your_spark_script.py


# pip install cassandra-driver

from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

import time

def create_keyspace(session, keyspace="clickstream"):
    cql = f"""
    CREATE KEYSPACE IF NOT EXISTS {keyspace}
    WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': '1'}}
    """
    session.execute(cql)
    print(f"Keyspace '{keyspace}' created or exists.")

def create_tables(session, keyspace="clickstream"):
    tables = {
        "time_agg": f"""
            CREATE TABLE IF NOT EXISTS {keyspace}.time_agg (
                window_start timestamp,
                window_end timestamp,
                page text,
                count bigint,
                PRIMARY KEY ((window_start), page)
            )
        """,

        "campaign_events": f"""
            CREATE TABLE IF NOT EXISTS {keyspace}.campaign_events (
                window_start timestamp,
                window_end timestamp,
                utm_campaign text,
                utm_source text,
                event_count bigint,
                PRIMARY KEY ((window_start), utm_campaign, utm_source)
            )
        """,

        "campaign_actions": f"""
            CREATE TABLE IF NOT EXISTS {keyspace}.campaign_actions (
                window_start timestamp,
                window_end timestamp,
                utm_campaign text,
                add_to_cart bigint,
                purchases bigint,
                PRIMARY KEY ((window_start), utm_campaign)
            )
        """,

        "product_views": f"""
            CREATE TABLE IF NOT EXISTS {keyspace}.product_views (
                window_start timestamp,
                window_end timestamp,
                product_id text,
                product_views bigint,
                PRIMARY KEY ((window_start), product_id)
            )
        """,

        "product_actions": f"""
            CREATE TABLE IF NOT EXISTS {keyspace}.product_actions (
                window_start timestamp,
                window_end timestamp,
                product_id text,
                views bigint,
                add_to_cart bigint,
                add_to_cart_rate double,
                PRIMARY KEY ((window_start), product_id)
            )
        """,

        "agg_duration": f"""
            CREATE TABLE IF NOT EXISTS {keyspace}.agg_duration (
                window_start timestamp,
                window_end timestamp,
                page text,
                avg_duration double,
                PRIMARY KEY ((window_start), page)
            )
        """,

        "product_purchases": f"""
            CREATE TABLE IF NOT EXISTS {keyspace}.product_purchases (
                window_start timestamp,
                window_end timestamp,
                product_id text,
                purchases bigint,
                PRIMARY KEY ((window_start), product_id)
            )
        """,

        "product_cart_additions": f"""
            CREATE TABLE IF NOT EXISTS {keyspace}.product_cart_additions (
                window_start timestamp,
                window_end timestamp,
                product_id text,
                cart_adds bigint,
                PRIMARY KEY ((window_start), product_id)
            )
        """,

        "website_views": f"""
            CREATE TABLE IF NOT EXISTS {keyspace}.website_views (
                window_start timestamp,
                window_end timestamp,
                views bigint,
                PRIMARY KEY (window_start)
            )
        """,

        "device_distribution": f"""
            CREATE TABLE IF NOT EXISTS {keyspace}.device_distribution (
                window_start timestamp,
                window_end timestamp,
                device_type text,
                views bigint,
                PRIMARY KEY ((window_start), device_type)
            )
        """
    }

    # # Kampagne
    # session.execute(f"""
    # CREATE TABLE IF NOT EXISTS {keyspace}.campaign_events (
    #     window_start timestamp,
    #     window_end timestamp,
    #     utm_campaign text,
    #     event_count bigint,
    #     PRIMARY KEY ((window_start), utm_campaign)
    # )
    # """)




    print(f"📦 Creating tables in keyspace '{keyspace}'...\n")
    for name, cql in tables.items():
        try:
            session.execute(cql)
            print(f"✅ Table '{name}' created or exists.")
        except Exception as e:
            print(f"❌ Failed to create table '{name}': {e}")  # wichtig!
    print("\n🏁 Table creation completed.")

def wait_for_cassandra(host, timeout=60):
    start = time.time()
    while True:
        try:
            cluster = Cluster([host])
            session = cluster.connect()
            print("✅ Connection to Cassandra successful.")
            return cluster, session
        except Exception as e:
            if time.time() - start > timeout:
                raise RuntimeError(f"❌ Timeout while waiting for Cassandra: {e}")
            print("⏳ Cassandra not yet ready, waiting...")
            time.sleep(5)

def main():
    cluster, session = wait_for_cassandra("cassandra")  # oder host.docker.internal wenn lokal

    keyspace = "clickstream"
    create_keyspace(session, keyspace)
    session.set_keyspace(keyspace)

    create_tables(session, keyspace)
    time.sleep(10)  # Cassandra Zeit geben, DDLs zu finalisieren
    session.shutdown()

if __name__ == "__main__":
    main()

