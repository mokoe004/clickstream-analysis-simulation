import dash
from dash import dcc, html
import dash_bootstrap_components as dbc
import pandas as pd
import plotly.express as px
from cassandra.cluster import Cluster

# Cassandra-Verbindung
def fetch_data(query):
    cluster = Cluster(["cassandra"])
    session = cluster.connect("clickstream")
    rows = session.execute(query)
    
    if not rows:
        print(f"⚠️ Abfrage ergab keine Daten: {query}")
        return pd.DataFrame()
    
    columns = rows.column_names
    df = pd.DataFrame(rows, columns=columns)

# Zum Debuggen
    debug = query.replace("SELECT * FROM ", "").strip()
    print(debug)
    print(df.columns) 
    
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
# df_sessions = fetch_data("SELECT * FROM session_durations")
df_devices = fetch_data("SELECT * FROM device_distribution")
# df_geo = fetch_data("SELECT * FROM geo_stats")
# df_login = fetch_data("SELECT * FROM login_stats")
# df_referrers = fetch_data("SELECT * FROM referrer_stats")
df_campaigns = fetch_data("SELECT * FROM campaign_events")
# df_campaign_conversion = fetch_data("SELECT * FROM campaign_conversions")

# Layout der App
app.layout = dbc.Container([
    html.H1("📊 Clickstream Campaign Dashboard", className="my-4"),

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
        figure=px.line(df_pages, x="window_start", y="views", title="Website Views über Zeit")
    ),

    # dcc.Graph(
    #     id="hist-session-duration",
    #     figure=px.histogram(df_sessions, x="duration_seconds", title="Session-Dauerverteilung")
    # ),

    # dcc.Graph(
    #     id="bar-page-types",
    #     figure=px.bar(df_pages, x="page", y="total_views", title="Meistbesuchte Seitentypen")
    # ),

    # dcc.Graph(
    #     id="bar-cities",
    #     figure=px.bar(df_geo, x="geo_city", y="visits", title="Top-Städte")
    # ),

    dcc.Graph(
        id="pie-devices",
        figure=px.pie(df_devices, names="device_type", values="views", title="Geräteverteilung")
    ),

    # dcc.Graph(
    #     id="pie-os",
    #     figure=px.pie(df_devices, names="os", values="count", title="OS-Verteilung")
    # ),

    # dcc.Graph(
    #     id="pie-browsers",
    #     figure=px.pie(df_devices, names="browser", values="count", title="Browser-Verteilung")
    # ),

    # dcc.Graph(
    #     id="pie-login",
    #     figure=px.pie(df_login, names="is_logged_in", values="count", title="Eingeloggt vs. nicht")
    # ),

    # dcc.Graph(
    #     id="bar-referrers",
    #     figure=px.bar(df_referrers, x="referrer", y="count", title="Referrer-Domains")
    # ),

    dcc.Graph(
        id="bar-campaigns",
        figure=px.bar(df_campaigns.groupby("utm_campaign", as_index=False)["event_count"].sum(),
                      x="utm_campaign", y="event_count", title="Top-UTM-Kampagnen")
    ),

    # dcc.Graph(
    #     id="bar-campaign-conversion",
    #     figure=px.bar(df_campaign_conversion, x="utm_campaign", y="conversion_rate",
    #                   title="Kampagnen-Konversionen")
    # ),

    dcc.Graph(
        id="line-campaign-traffic",
        figure=px.line(df_campaigns, x="window_start", y="event_count", color="utm_campaign",
                       title="Kampagnen-Traffic-Trends über Zeit")
    )

    # dcc.Graph(
    #     id="line-campaigns",
    #     figure=px.line(
    #         df,
    #         x="window_start",
    #         y="event_count",
    #         color="utm_campaign",
    #         title="Event Count über Zeit pro Kampagne"
    #     )
    # ),

    # dcc.Graph(
    #     id="bar-sources",
    #     figure=px.bar(
    #         df.groupby("utm_source", as_index=False)["event_count"].sum(),
    #         x="utm_source",
    #         y="event_count",
    #         title="Total Event Count pro Quelle (utm_source)"
    #     )
    # )
], fluid=True)

# App starten
if __name__ == "__main__":
    print("Open http://localhost:8050 in your Browser.")
    app.run(host="0.0.0.0", debug=True)

