import pandas as pd
import plotly.express as px

def create_campaign_traffic_plot(df_campaigns: pd.DataFrame, top_n: int = 5):
    """
    Erstellt ein Liniendiagramm der Top-N-Kampagnen mit Tagesaggregation.
    """
    required_cols = {"window_start", "utm_campaign", "event_count"}
    if not required_cols.issubset(df_campaigns.columns):
        raise ValueError(f"Fehlende Spalten: {required_cols - set(df_campaigns.columns)}")

    # Datum extrahieren (nur Tagesgenauigkeit)
    df_campaigns["window_date"] = pd.to_datetime(df_campaigns["window_start"]).dt.date

    # Top-N Kampagnen bestimmen
    top_campaigns = (
        df_campaigns.groupby("utm_campaign", as_index=False)["event_count"]
        .sum()
        .sort_values("event_count", ascending=False)
        .head(top_n)["utm_campaign"]
    )

    # Nur Top-Kampagnen behalten
    df_top = df_campaigns[df_campaigns["utm_campaign"].isin(top_campaigns)]

    # Tägliche Aggregation
    df_daily = (
        df_top.groupby(["window_date", "utm_campaign"], as_index=False)["event_count"]
        .sum()
    )

    # Plot erzeugen
    fig = px.line(
        df_daily,
        x="window_date",
        y="event_count",
        color="utm_campaign",
        title=f"Kampagnen-Traffic-Trends (Top {top_n}, täglich aggregiert)"
    )

    fig.update_layout(
        xaxis_title="Datum",
        yaxis_title="Event Count",
        legend_title_text="utm_campaign"
    )

    return fig
