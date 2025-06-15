import pandas as pd
import plotly.express as px
from cassandra.cluster import Cluster

def fetch_live_product_purchases(minutes=10, top_n=5):
    """
    Holt die letzten N Minuten an Produktkäufen aus Cassandra.
    """
    cluster = Cluster(["cassandra"])
    session = cluster.connect("clickstream")

    # Zeitgrenze berechnen
    from datetime import datetime, timedelta
    time_limit = datetime.utcnow() - timedelta(minutes=minutes)

    query = f"""
        SELECT * FROM product_purchases
        WHERE window_start >= '{time_limit.isoformat()}'
        ALLOW FILTERING;
    """
    rows = session.execute(query)
    columns = rows.column_names
    df = pd.DataFrame(rows, columns=columns)
    session.shutdown()

    if df.empty:
        return px.bar(title="Keine Daten verfügbar")

    # Top-Produkte nach Käufen aggregieren
    df_grouped = df.groupby("product_id", as_index=False)["purchases"].sum()
    df_top = df_grouped.sort_values("purchases", ascending=False).head(top_n)

    # Plot erstellen
    fig = px.bar(
        df_top,
        x="product_id",
        y="purchases",
        title=f"Top {top_n} Produkte (letzte {minutes} Minuten)"
    )

    return fig
