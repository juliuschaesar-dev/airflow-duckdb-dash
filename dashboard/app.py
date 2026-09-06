"""Plotly Dash dashboard reading crypto market data from DuckDB (read-only).

Runs as a separate service from Airflow — Airflow is the only writer to the
DuckDB file, this app only ever opens it with read_only=True.
"""
from __future__ import annotations

import os

import duckdb
import pandas as pd
import plotly.express as px
from dash import Dash, Input, Output, dcc, html

from constants import (
    CRYPTO_MARKET_DATA,
    FLAG_DOWN,
    FLAG_FLAT,
    FLAG_UNKNOWN,
    FLAG_UP,
    OUTPUT_COLUMNS,
)

DB_PATH = os.environ.get("CRYPTO_DB_PATH", "/app/data/crypto.duckdb")
REFRESH_INTERVAL_MS = int(os.environ["CRYPTO_REFRESH_MS"])


def get_connection() -> duckdb.DuckDBPyConnection | None:
    if not os.path.exists(DB_PATH):
        return None
    return duckdb.connect(DB_PATH, read_only=True)


def load_history() -> pd.DataFrame:
    con = get_connection()
    if con is None:
        return pd.DataFrame()
    try:
        return con.execute(
            f"""
            SELECT {", ".join(OUTPUT_COLUMNS)}
            FROM {CRYPTO_MARKET_DATA}
            ORDER BY snapshot_ts
            """
        ).fetch_df()
    except duckdb.CatalogException:
        return pd.DataFrame()
    finally:
        con.close()


def latest_snapshot(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    latest_ts = df["snapshot_ts"].max()
    return df[df["snapshot_ts"] == latest_ts]


app = Dash(__name__)
app.title = "Crypto Market Dashboard"
server = app.server  # exposed for gunicorn (see dashboard/Dockerfile)

app.layout = html.Div(
    className="app-container",
    children=[
        html.H1("Crypto Market Dashboard"),
        html.P("Source: CoinGecko → Airflow → DuckDB. Refreshes automatically."),
        dcc.Interval(id="refresh-interval", interval=REFRESH_INTERVAL_MS, n_intervals=0),
        html.Div(id="empty-state"),
        html.Div(
            className="controls",
            children=[
                html.Label("Coins to chart:"),
                dcc.Dropdown(id="coin-selector", multi=True, placeholder="Select coins..."),
            ],
        ),
        dcc.Graph(id="price-line-chart"),
        html.Div(
            className="chart-row",
            children=[
                dcc.Graph(id="gainers-losers-bar", style={"flex": 1}),
                dcc.Graph(id="market-cap-treemap", style={"flex": 1}),
            ],
        ),
    ],
)


@app.callback(
    Output("coin-selector", "options"),
    Output("coin-selector", "value"),
    Output("empty-state", "children"),
    Input("refresh-interval", "n_intervals"),
)
def refresh_coin_options(_n):
    df = load_history()
    if df.empty:
        return [], [], html.Div(
            "No data yet — waiting for the Airflow pipeline's first successful run.",
            className="empty-state",
        )
    latest = latest_snapshot(df).sort_values("market_cap", ascending=False)
    options = [{"label": f"{row['name']} ({row['symbol'].upper()})", "value": row["coin_id"]}
               for _, row in latest.iterrows()]
    default_value = latest["coin_id"].head(5).tolist()
    return options, default_value, ""


@app.callback(
    Output("price-line-chart", "figure"),
    Input("coin-selector", "value"),
    Input("refresh-interval", "n_intervals"),
)
def update_price_line_chart(selected_coins, _n):
    df = load_history()
    if df.empty:
        return px.line(title="Price over time (no data yet)")
    if selected_coins:
        df = df[df["coin_id"].isin(selected_coins)]
    fig = px.line(
        df,
        x="snapshot_ts",
        y="current_price",
        color="name",
        markers=True,
        title="Price over time",
        labels={"snapshot_ts": "Snapshot", "current_price": "Price (USD)", "name": "Coin"},
    )
    return fig


@app.callback(
    Output("gainers-losers-bar", "figure"),
    Input("refresh-interval", "n_intervals"),
)
def update_gainers_losers_bar(_n):
    df = load_history()
    if df.empty:
        return px.bar(title="Top gainers / losers (no data yet)")
    latest = latest_snapshot(df).dropna(subset=["price_change_percentage_24h"])
    top = pd.concat(
        [latest.nlargest(5, "price_change_percentage_24h"),
         latest.nsmallest(5, "price_change_percentage_24h")]
    ).drop_duplicates(subset=["coin_id"]).sort_values("price_change_percentage_24h")
    fig = px.bar(
        top,
        x="price_change_percentage_24h",
        y="name",
        orientation="h",
        color="price_change_flag",
        color_discrete_map={
            FLAG_UP: "#2ca02c",
            FLAG_DOWN: "#d62728",
            FLAG_FLAT: "#7f7f7f",
            FLAG_UNKNOWN: "#bbbbbb",
        },
        title="Top gainers / losers (24h)",
        labels={"price_change_percentage_24h": "24h change (%)", "name": "Coin"},
    )
    return fig


@app.callback(
    Output("market-cap-treemap", "figure"),
    Input("refresh-interval", "n_intervals"),
)
def update_market_cap_treemap(_n):
    df = load_history()
    if df.empty:
        return px.treemap(title="Market cap comparison (no data yet)")
    latest = latest_snapshot(df)
    fig = px.treemap(
        latest,
        path=[px.Constant("All coins"), "name"],
        values="market_cap",
        color="price_change_percentage_24h",
        color_continuous_scale="RdYlGn",
        color_continuous_midpoint=0,
        title="Market cap comparison",
    )
    return fig


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8050, debug=False)
