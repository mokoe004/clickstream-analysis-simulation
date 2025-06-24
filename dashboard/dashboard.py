# dashboard.py

import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import dash_bootstrap_components as dbc
import pandas as pd
import plotly.express as px

from data_fetch import fetch_data
from campaign_traffic_plot import create_campaign_traffic_plot
from website_views_plot import create_website_views_plot
from live_top_products_plot import fetch_live_product_purchases

# Dash App initialisieren
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
app.title = "Clickstream Dashboard"

KEYSPACE = "clickstream"

# Daten laden
df_views = fetch_data("SELECT * FROM product_views", KEYSPACE, "product_views")
df_cart = fetch_data("SELECT * FROM product_cart_additions", KEYSPACE, "product_cart_additions")
df_purchases = fetch_data("SELECT * FROM product_purchases", KEYSPACE, "product_purchases")
df_pages = fetch_data("SELECT * FROM website_views", KEYSPACE, "website_views")
df_devices = fetch_data("SELECT * FROM device_distribution", KEYSPACE, "device_distribution")
df_campaigns = fetch_data("SELECT * FROM campaign_events", KEYSPACE, "campaign_events")
df_duration = fetch_data("SELECT * FROM agg_duration", KEYSPACE, "agg_duration")
df_campaign_actions = fetch_data("SELECT * FROM campaign_actions", KEYSPACE, "campaign_actions")
df_timeagg = fetch_data("SELECT * FROM time_agg", KEYSPACE, "time_agg")

# Batch-Daten laden
df_views_batch = fetch_data("SELECT * FROM product_views_batch", KEYSPACE, "product_views_batch")
df_cart_batch = fetch_data("SELECT * FROM product_cart_additions_batch", KEYSPACE, "product_cart_additions_batch")
df_purchases_batch = fetch_data("SELECT * FROM product_purchases_batch", KEYSPACE, "product_purchases_batch")
df_duration_batch = fetch_data("SELECT * FROM agg_duration_batch", KEYSPACE, "agg_duration_batch")
df_timeagg_batch = fetch_data("SELECT * FROM time_agg_batch", KEYSPACE, "time_agg_batch")


# Safe aggregations
if not df_duration.empty and {"page", "avg_duration"}.issubset(df_duration.columns):
    df_duration = df_duration.groupby("page", as_index=False)["avg_duration"].mean()
else:
    df_duration = pd.DataFrame(columns=["page", "avg_duration"])

if not df_timeagg.empty and {"window_start", "page", "count"}.issubset(df_timeagg.columns):
    df_timeagg = df_timeagg.groupby(["window_start", "page"], as_index=False)["count"].sum()
else:
    df_timeagg = pd.DataFrame(columns=["window_start", "page", "count"])

# Batch safe aggregations
if not df_duration_batch.empty and {"page", "avg_duration"}.issubset(df_duration_batch.columns):
    df_duration_batch = df_duration_batch.groupby("page", as_index=False)["avg_duration"].mean()
else:
    df_duration_batch = pd.DataFrame(columns=["page", "avg_duration"])

if not df_timeagg_batch.empty and {"window_start", "page", "count"}.issubset(df_timeagg_batch.columns):
    df_timeagg_batch = df_timeagg_batch.groupby(["window_start", "page"], as_index=False)["count"].sum()
else:
    df_timeagg_batch = pd.DataFrame(columns=["window_start", "page", "count"])

# Layout of App no
app.layout = dbc.Container([
    html.H1("📊 Clickstream Campaign Dashboard", className="my-4"),

    dcc.Interval(id="live-interval", interval=10 * 1000, n_intervals=0),
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
        figure=px.bar(
            df_campaigns.groupby("utm_campaign", as_index=False)["event_count"].sum()
            if not df_campaigns.empty else pd.DataFrame(columns=["utm_campaign", "event_count"]),
            x="utm_campaign", y="event_count", title="Top-UTM-Kampagnen"
        )
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
        if not df_campaign_actions.empty else px.bar(pd.DataFrame(columns=["utm_campaign", "add_to_cart", "purchases"]))
    ),

    dcc.Graph(
        id="line-timeagg",
        figure=px.line(df_timeagg, x="window_start", y="count", color="page",
                       title="Seitenbesuche über Zeit (time_agg)")
    ),

    html.Hr(),
    html.H2("📦 Batch Visualisierung", className="my-4"),

    dcc.Graph(
        id="bar-top-products-batch",
        figure=px.bar(df_purchases_batch, x="product_id", y="purchases", title="Batch: Top-Produkte nach Käufen")
    ),

    dcc.Graph(
        id="bar-cart-batch",
        figure=px.bar(df_cart_batch, x="product_id", y="cart_adds", title="Batch: Top-Produkte im Warenkorb")
    ),

    dcc.Graph(
        id="bar-views-batch",
        figure=px.bar(df_views_batch, x="product_id", y="product_views", title="Batch: Meistgesehene Produkte")
    ),

    dcc.Graph(
        id="bar-duration-batch",
        figure=px.bar(df_duration_batch, x="page", y="avg_duration", color="page",
                      title="Batch: Ø Verweildauer pro Seite")
    ),

    dcc.Graph(
        id="line-timeagg-batch",
        figure=px.line(df_timeagg_batch, x="window_start", y="count", color="page",
                       title="Batch: Seitenbesuche über Zeit")
    )

], fluid=True)


@app.callback(
    Output("live-top-products", "figure"),
    Input("live-interval", "n_intervals")
)
def update_live_top_products(n):
    return fetch_live_product_purchases(hours_back=6, top_n=5)

if __name__ == "__main__":
    print("Open http://localhost:8050 in your Browser.")
    app.run(host="0.0.0.0", debug=True)
