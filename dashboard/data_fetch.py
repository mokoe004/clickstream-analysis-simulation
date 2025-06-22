"""
data_fetch.py

This module provides utility functions for querying a Cassandra database
and returning results as Pandas DataFrames. It includes logic to safely
handle empty query results by returning empty DataFrames with the correct
schema based on the table metadata.

Functions:
- get_table_columns(keyspace, table_name): Returns the column names of a given Cassandra table.
- fetch_data(query, keyspace, table_name): Executes a CQL query and returns a DataFrame. If the query
  returns no rows, a structured empty DataFrame with correct columns is returned.
"""

from cassandra.cluster import Cluster
import pandas as pd

CASSANDRA_HOSTS = ["cassandra"]

def get_table_columns(keyspace, table_name):
    """Liest Spaltennamen aus dem Cassandra-Tabellenschema."""
    cluster = Cluster(CASSANDRA_HOSTS)
    session = cluster.connect()
    metadata = cluster.metadata

    try:
        table_meta = metadata.keyspaces[keyspace].tables[table_name]
        return list(table_meta.columns.keys())
    except KeyError:
        print(f"❌ Tabelle '{table_name}' nicht gefunden in Keyspace '{keyspace}'")
        return []
    finally:
        session.shutdown()

def fetch_data(query, keyspace, table_name):
    """Führt eine CQL-Abfrage aus und gibt DataFrame zurück. Gibt bei leeren Ergebnissen leeren DF mit richtigen Spalten zurück."""
    cluster = Cluster(CASSANDRA_HOSTS)
    session = cluster.connect(keyspace)

    rows = session.execute(query)

    if not rows:
        print(f"⚠️ Abfrage ergab keine Daten: {query}")
        columns = get_table_columns(keyspace, table_name)
        session.shutdown()
        return pd.DataFrame(columns=columns)

    columns = rows.column_names
    df = pd.DataFrame(rows, columns=columns)

    session.shutdown()
    return df
