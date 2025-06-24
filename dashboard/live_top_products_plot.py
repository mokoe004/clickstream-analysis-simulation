import pandas as pd
import plotly.express as px
from cassandra.cluster import Cluster
from datetime import datetime, timedelta

def fetch_live_product_purchases(hours_back=6, top_n=5):
    cluster = Cluster(["cassandra"])
    session = cluster.connect("clickstream")

    now = datetime.utcnow()
    start_time = now.replace(minute=0, second=0, microsecond=0) - timedelta(hours=hours_back)

    query = f"""
        SELECT product_id, window_start, purchases
        FROM product_purchases
        WHERE window_start >= '{start_time.isoformat()}'
        ALLOW FILTERING;
    """
    rows = session.execute(query)
    df = pd.DataFrame(rows.all(), columns=["product_id", "window_start", "purchases"])
    session.shutdown()

    with open("debug.log", "a") as f:
        f.write(f"Query: {query}\n")
        f.write(f"DataFrame shape: {df.shape}\n")
        f.write(f"DataFrame head:\n{df.head()}\n")

    if df.empty:
        return px.bar(title="Keine Daten verfügbar")

    df["window_start"] = pd.to_datetime(df["window_start"])

    # Bestimme Top-N-Produkte nach Kaufanzahl
    top_products = (
        df.groupby("product_id")["purchases"]
        .sum()
        .nlargest(top_n)
        .index
    )
    df_top = df[df["product_id"].isin(top_products)]

    # Alle Stunden als Zeitachse generieren
    all_hours = pd.date_range(start=start_time, end=now, freq="h")

    # Kombiniere jedes Top-Produkt mit allen Stunden
    product_time_grid = pd.MultiIndex.from_product(
        [top_products, all_hours], names=["product_id", "window_start"]
    ).to_frame(index=False)

    # Merge mit echten Daten
    df_merged = pd.merge(
        product_time_grid,
        df_top,
        on=["product_id", "window_start"],
        how="left"
    )

    # Fehlende Käufe mit 0 auffüllen
    df_merged["purchases"] = df_merged["purchases"].fillna(0)

    # Plot erstellen
    fig = px.line(
        df_merged,
        x="window_start",
        y="purchases",
        color="product_id",
        title=f"Käufe der Top {top_n} Produkte über Zeit (letzte {hours_back} Stunden). Aktualisiert sich alle 10 Sekunden."
    )
    fig.update_layout(
        xaxis_title="Zeit (UTC)",
        yaxis_title="Anzahl Käufe",
        legend_title_text="Produkt-ID"
    )

    return fig
