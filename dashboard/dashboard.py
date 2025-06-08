import dash
from dash import dcc, html
import dash_bootstrap_components as dbc
import pandas as pd
import plotly.express as px
from cassandra.cluster import Cluster

# Cassandra-Verbindung
def fetch_campaign_events():
    cluster = Cluster(["cassandra"])  # ggf. "cassandra" in Docker Compose
    session = cluster.connect("clickstream")
    rows = session.execute("SELECT * FROM campaign_events")

    # Umwandeln in DataFrame
    df = pd.DataFrame(rows)
    session.shutdown()

    # Sortierung für Zeitachsen
    df = df.sort_values("window_start")
    return df

# Dash App initialisieren
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
app.title = "Campaign Dashboard"

# Daten laden
df = fetch_campaign_events()

# Layout der App
app.layout = dbc.Container([
    html.H1("📊 Clickstream Campaign Dashboard", className="my-4"),

    dcc.Graph(
        id="line-campaigns",
        figure=px.line(
            df,
            x="window_start",
            y="event_count",
            color="utm_campaign",
            title="Event Count über Zeit pro Kampagne"
        )
    ),

    dcc.Graph(
        id="bar-sources",
        figure=px.bar(
            df.groupby("utm_source", as_index=False)["event_count"].sum(),
            x="utm_source",
            y="event_count",
            title="Total Event Count pro Quelle (utm_source)"
        )
    )
], fluid=True)

# App starten
if __name__ == "__main__":
    print("Open http://localhost:8050 in your Browser.")
    app.run(host="0.0.0.0", debug=True)

