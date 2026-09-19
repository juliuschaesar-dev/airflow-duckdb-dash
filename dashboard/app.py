"""Plotly Dash dashboard reading crypto market data from DuckDB (read-only).

Runs as a separate service from Airflow — Airflow is the only writer to the
DuckDB file, this app only ever opens it with read_only=True.
"""
from __future__ import annotations

import io
import os

import duckdb
import pandas as pd
import plotly.express as px
from dash import Dash, Input, Output, dcc, html

from constants import CRYPTO_MARKET_DATA, OUTPUT_COLUMNS, PRICE_CHANGE_COLORS

DB_PATH = os.environ.get("CRYPTO_DB_PATH", "/app/data/crypto.duckdb")
REFRESH_INTERVAL_MS = int(os.environ["CRYPTO_REFRESH_MS"])
NO_DATA_SUFFIX = " (no data yet)"


def get_connection() -> duckdb.DuckDBPyConnection | None:
    if not os.path.exists(DB_PATH):
        return None
    try:
        return duckdb.connect(DB_PATH, read_only=True)
    except duckdb.Error:
        return None


def load_history() -> pd.DataFrame:
    con = get_connection()
    if con is None:
        return pd.DataFrame()
    try:
        df = con.execute(
            f"""
            SELECT {", ".join(OUTPUT_COLUMNS)}
            FROM {CRYPTO_MARKET_DATA}
            ORDER BY snapshot_ts
            """
        ).fetch_df()
        df["snapshot_ts"] = pd.to_datetime(df["snapshot_ts"])
        return df
    except duckdb.Error:
        # e.g. CatalogException: the table doesn't exist yet.
        return pd.DataFrame()
    finally:
        con.close()


def history_from_store(data: str | None) -> pd.DataFrame:
    """Deserialize the shared dcc.Store payload back into a DataFrame."""
    if not data:
        return pd.DataFrame()
    df = pd.read_json(io.StringIO(data), orient="split")
    df["snapshot_ts"] = pd.to_datetime(df["snapshot_ts"])
    return df


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
        dcc.Store(id="history-store"),
        html.Div(id="empty-state"),
        html.Div(
            className="controls-row",
            children=[
                html.Div(
                    className="controls",
                    children=[
                        html.Label("Coins to chart:"),
                        dcc.Dropdown(id="coin-selector", multi=True, placeholder="Select coins..."),
                    ],
                ),
                html.Div(
                    className="controls",
                    children=[
                        html.Label("Date range (price over time):"),
                        html.Br(),
                        dcc.DatePickerRange(id="date-range-selector"),
                    ],
                ),
            ],
        ),
        dcc.Graph(id="price-line-chart"),
        html.Div(
            className="chart-row",
            children=[
                dcc.Graph(id="gainers-losers-bar"),
                dcc.Graph(id="market-cap-treemap"),
            ],
        ),
    ],
)


@app.callback(
    Output("history-store", "data"),
    Input("refresh-interval", "n_intervals"),
)
def refresh_history_store(_n):
    df = load_history()
    if df.empty:
        return None
    return df.to_json(date_format="iso", orient="split")


@app.callback(
    Output("coin-selector", "options"),
    Output("coin-selector", "value"),
    Output("empty-state", "children"),
    Input("history-store", "data"),
)
def refresh_coin_options(data):
    df = history_from_store(data)
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
    Output("date-range-selector", "min_date_allowed"),
    Output("date-range-selector", "max_date_allowed"),
    Output("date-range-selector", "start_date"),
    Output("date-range-selector", "end_date"),
    Input("history-store", "data"),
)
def refresh_date_range(data):
    df = history_from_store(data)
    if df.empty:
        return None, None, None, None
    min_date = df["snapshot_ts"].min().date()
    max_date = df["snapshot_ts"].max().date()
    default_start = max(min_date, max_date - pd.Timedelta(days=30))
    return min_date, max_date, default_start, max_date


@app.callback(
    Output("price-line-chart", "figure"),
    Input("history-store", "data"),
    Input("coin-selector", "value"),
    Input("date-range-selector", "start_date"),
    Input("date-range-selector", "end_date"),
)
def update_price_line_chart(data, selected_coins, start_date, end_date):
    df = history_from_store(data)
    if df.empty:
        return px.line(title="Price over time" + NO_DATA_SUFFIX)
    if selected_coins:
        df = df[df["coin_id"].isin(selected_coins)]
    if start_date:
        df = df[df["snapshot_ts"] >= pd.to_datetime(start_date)]
    if end_date:
        df = df[df["snapshot_ts"] < pd.to_datetime(end_date) + pd.Timedelta(days=1)]
    fig = px.line(
        df,
        x="snapshot_ts",
        y="current_price",
        color="name",
        markers=True,
        title="Price over time",
        labels={"snapshot_ts": "Snapshot", "current_price": "Price (USD)", "name": "Coin"},
    )
    fig.update_xaxes(dtick="D1", tickformat="%b %d, %Y")
    return fig


@app.callback(
    Output("gainers-losers-bar", "figure"),
    Input("history-store", "data"),
)
def update_gainers_losers_bar(data):
    df = history_from_store(data)
    if df.empty:
        return px.bar(title="Top gainers / losers (24h)" + NO_DATA_SUFFIX)
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
        color_discrete_map=PRICE_CHANGE_COLORS,
        text="price_change_percentage_24h",
        title="Top gainers / losers (24h)",
        labels={
            "price_change_percentage_24h": "24h change",
            "name": "Coin",
            "price_change_flag": "Trend",
        },
    )
    fig.update_traces(texttemplate="%{text:+.0f}%", textposition="outside", cliponaxis=False)
    max_change = top["price_change_percentage_24h"].abs().max()
    fig.update_xaxes(range=[-max_change * 1.25, max_change * 1.25])
    return fig


def format_market_cap(value: float) -> str:
    if value >= 1e12:
        return f"${value / 1e12:.2f}T"
    if value >= 1e9:
        return f"${value / 1e9:.0f}B"
    if value >= 1e6:
        return f"${value / 1e6:.0f}M"
    return f"${value:,.0f}"


@app.callback(
    Output("market-cap-treemap", "figure"),
    Input("history-store", "data"),
    Input("coin-selector", "value"),
)
def update_market_cap_treemap(data, selected_coins):
    df = history_from_store(data)
    if df.empty:
        return px.treemap(title="Market cap comparison (24h)" + NO_DATA_SUFFIX)
    latest = latest_snapshot(df)
    if selected_coins:
        latest = latest[latest["coin_id"].isin(selected_coins)]
    latest = latest.copy()
    latest["market_cap_label"] = latest["market_cap"].apply(format_market_cap)
    latest["change_label"] = latest["price_change_percentage_24h"].apply(
        lambda pct: f"{pct:+.1f}%" if pd.notna(pct) else "n/a"
    )
    fig = px.treemap(
        latest,
        path=[px.Constant("All coins"), "name"],
        values="market_cap",
        color="price_change_percentage_24h",
        color_continuous_scale="RdYlGn",
        color_continuous_midpoint=0,
        title="Market cap comparison (24h)",
        labels={"price_change_percentage_24h": "24h change (%)"},
        custom_data=["market_cap_label", "change_label"],
    )
    fig.update_traces(texttemplate="<b>%{label}</b><br>%{customdata[0]}<br>%{customdata[1]}")
    return fig


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8050, debug=False)
