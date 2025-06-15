import pandas as pd
import plotly.express as px


def create_website_views_plot(df_views: pd.DataFrame) -> px.line:
    """
    Erstellt ein Liniendiagramm der Website Views, täglich aggregiert.

    :param df_views: DataFrame mit window_start und views
    :return: plotly Figure
    """
    if "window_start" not in df_views.columns or "views" not in df_views.columns:
        raise ValueError("DataFrame benötigt Spalten 'window_start' und 'views'.")

    # Datum extrahieren
    df_views["window_date"] = pd.to_datetime(df_views["window_start"]).dt.date

    # Tägliche Aggregation
    df_daily = df_views.groupby("window_date", as_index=False)["views"].sum()

    # Plot erzeugen
    fig = px.line(
        df_daily,
        x="window_date",
        y="views",
        title="Website Views pro Tag (täglich aggregiert)"
    )

    fig.update_layout(
        xaxis_title="Datum",
        yaxis_title="Views",
        showlegend=False
    )

    return fig
