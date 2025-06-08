

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
    # Table für time_agg
    session.execute(f"""
    CREATE TABLE IF NOT EXISTS {keyspace}.time_agg (
        window_start timestamp,
        window_end timestamp,
        page text,
        count bigint,
        PRIMARY KEY ((window_start), page)
    )
    """)

    # Table für campaign_events
    session.execute(f"""
    CREATE TABLE IF NOT EXISTS {keyspace}.campaign_events (
        window_start timestamp,
        window_end timestamp,
        utm_campaign text,
        utm_source text,
        event_count bigint,
        PRIMARY KEY ((window_start), utm_campaign, utm_source)
    )
    """)

    # Table für campaign_actions
    session.execute(f"""
    CREATE TABLE IF NOT EXISTS {keyspace}.campaign_actions (
        window_start timestamp,
        window_end timestamp,
        utm_campaign text,
        add_to_cart bigint,
        purchases bigint,
        PRIMARY KEY ((window_start), utm_campaign)
    )
    """)

    # Table für product_views
    session.execute(f"""
    CREATE TABLE IF NOT EXISTS {keyspace}.product_views (
        window_start timestamp,
        window_end timestamp,
        product_id text,
        product_views bigint,
        PRIMARY KEY ((window_start), product_id)
    )
    """)

    # Table für product_actions
    session.execute(f"""
    CREATE TABLE IF NOT EXISTS {keyspace}.product_actions (
        window_start timestamp,
        window_end timestamp,
        product_id text,
        views bigint,
        add_to_cart bigint,
        add_to_cart_rate double,
        PRIMARY KEY ((window_start), product_id)
    )
    """)

    # Table für agg_duration
    session.execute(f"""
    CREATE TABLE IF NOT EXISTS {keyspace}.agg_duration (
        window_start timestamp,
        window_end timestamp,
        page text,
        avg_duration double,
        PRIMARY KEY ((window_start), page)
    )
    """)

    print("Tables created or exist.")

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
    session.shutdown()

if __name__ == "__main__":
    main()
    
