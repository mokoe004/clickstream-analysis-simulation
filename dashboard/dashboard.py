import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import dash_bootstrap_components as dbc
import pandas as pd
import plotly.express as px
from cassandra.cluster import Cluster

from campaign_traffic_plot import create_campaign_traffic_plot
from website_views_plot import create_website_views_plot
from live_top_products_plot import fetch_live_product_purchases


# Cassandra-Verbindung
def fetch_data(query):
    cluster = Cluster(["cassandra"])  # Cassandra-Host anpassen falls nötig
    session = cluster.connect("clickstream")
    rows = session.execute(query)

    if not rows:
        print(f"⚠️ Abfrage ergab keine Daten: {query}")
        return pd.DataFrame()

    columns = rows.column_names
    df = pd.DataFrame(rows, columns=columns)

    session.shutdown()
    return df


# Dash App initialisieren
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
app.title = "Campaign Dashboard"

# Daten laden
df_views = fetch_data("SELECT * FROM product_views")
df_cart = fetch_data("SELECT * FROM product_cart_additions")
df_purchases = fetch_data("SELECT * FROM product_purchases")
df_pages = fetch_data("SELECT * FROM website_views")
df_devices = fetch_data("SELECT * FROM device_distribution")
df_campaigns = fetch_data("SELECT * FROM campaign_events")

# Neue Aggregationen
df_duration = fetch_data("SELECT * FROM agg_duration")
df_campaign_actions = fetch_data("SELECT * FROM campaign_actions")
df_timeagg = fetch_data("SELECT * FROM time_agg")

# Optionales Gruppieren/Aggregieren (nur bei Bedarf)
df_duration = df_duration.groupby("page", as_index=False)["avg_duration"].mean()
df_timeagg = df_timeagg.groupby(["window_start", "page"], as_index=False)["count"].sum()


# Layout der App
app.layout = dbc.Container([
    html.H1("📊 Clickstream Campaign Dashboard", className="my-4"),

    # Intervall-Komponente hinzufügen (alle 60 Sekunden)
    dcc.Interval(id="live-interval", interval=60 * 1000, n_intervals=0),

    # Live-Graph-Komponente
    dcc.Graph(id="live-top-products"),

    dcc.Graph(
        id="bar-top-products",
        figure=px.bar(df_purchases, x="product_id", y="purchases", title="Top-Produkte nach Käufen")
    ),

    dcc.Graph(
        id="bar-top-cart",
        figure=px.bar(df_cart, x="product_id", y="cart_adds", title="Top-Produkte im Warenkorb")
    ),

    dcc.Graph(
        id="bar-top-views",
        figure=px.bar(df_views, x="product_id", y="product_views", title="Meistgesehene Produkte")
    ),

    dcc.Graph(
        id="line-views",
        figure=create_website_views_plot(df_pages)
    ),

    dcc.Graph(
        id="pie-devices",
        figure=px.pie(df_devices, names="device_type", values="views", title="Geräteverteilung")
    ),

    dcc.Graph(
        id="bar-campaigns",
        figure=px.bar(df_campaigns.groupby("utm_campaign", as_index=False)["event_count"].sum(),
                      x="utm_campaign", y="event_count", title="Top-UTM-Kampagnen")
    ),

    dcc.Graph(
        id="line-campaign-traffic",
        figure=create_campaign_traffic_plot(df_campaigns, top_n=5)
    ),

    dcc.Graph(
        id="bar-duration",
        figure=px.bar(df_duration, x="page", y="avg_duration", color="page",
                      title="Ø Verweildauer pro Seite (agg_duration)")
    ),

    dcc.Graph(
        id="grouped-campaign-actions",
        figure=px.bar(df_campaign_actions, x="utm_campaign", y=["add_to_cart", "purchases"],
                      barmode="group", title="Kampagnen-Performance: Warenkorb vs Käufe")
    ),

    dcc.Graph(
        id="line-timeagg",
        figure=px.line(df_timeagg, x="window_start", y="count", color="page",
                       title="Seitenbesuche über Zeit (time_agg)")
    )
], fluid=True)

@app.callback(
    Output("live-top-products", "figure"),
    Input("live-interval", "n_intervals")
)
def update_live_top_products(n):
    return fetch_live_product_purchases(minutes=10, top_n=5)


# App starten
if __name__ == "__main__":
    print("Open http://localhost:8050 in your Browser.")
    app.run(host="0.0.0.0", debug=True)
