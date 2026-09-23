"""Plotly Dash dashboard reading crypto market data from DuckDB (read-only).

Runs as a separate service from Airflow — Airflow is the only writer to the
DuckDB file, this app only ever opens it with read_only=True.

The pipeline lands one snapshot per day, so every view is driven by the date
range picker: the range start/end bound the time series (relative
performance, watchlist trends) and the latest snapshot on or before the range
end drives the point-in-time views (KPIs, top movers, market cap map).
"""
from __future__ import annotations

import io
import os

import duckdb
import pandas as pd
import plotly.graph_objects as go
from dash import Dash, Input, Output, State, dcc, html

from constants import (
    CRYPTO_MARKET_DATA,
    FLAG_DOWN,
    FLAG_FLAT,
    FLAG_UP,
    OUTPUT_COLUMNS,
    PRICE_CHANGE_COLORS,
)

DB_PATH = os.environ.get("CRYPTO_DB_PATH", "/app/data/crypto.duckdb")
REFRESH_INTERVAL_MS = int(os.environ["CRYPTO_REFRESH_MS"])

# Coin chips offered in the filter bar (top N by market cap in the latest
# snapshot) and how many of them start selected.
CHIP_COUNT = 10
DEFAULT_SELECTED = 5
TOP_MOVERS_COUNT = 5
DEFAULT_RANGE_DAYS = 30
# Market cap map colors saturate at ±this many percent of 24h change.
TREEMAP_COLOR_LIMIT = 3.0
NO_COINS_MESSAGE = "Select at least one coin"

# Categorical series colors, assigned by chip position (market cap order) so a
# coin keeps its color while the selection changes. Validated for the dark
# card surface (#141821): lightness band, chroma, CVD and contrast all pass
# for neighbouring slots. Some non-neighbours sit close (orange/gold,
# indigo/violet), so identity never rests on color alone: lines carry direct
# labels and watchlist rows carry the name.
SERIES_COLORS = [
    "#d4740a",  # orange
    "#7b7ce6",  # indigo
    "#26a17b",  # teal
    "#b38a0c",  # gold
    "#3d8fd6",  # blue
    "#d0569a",  # pink
    "#76993a",  # olive
    "#9a66e8",  # violet
    "#a8683a",  # brown
    "#2a9fb3",  # cyan
]

TEXT_PRIMARY = "#e8eaf0"
TEXT_SECONDARY = "#9aa3b5"
TEXT_MUTED = "#6b7385"
GRID_COLOR = "#1f2430"
FONT_SANS = "Inter, -apple-system, 'Segoe UI', Roboto, sans-serif"
FONT_MONO = "'JetBrains Mono', ui-monospace, Consolas, monospace"


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
            SELECT {", ".join(OUTPUT_COLUMNS)}, ingested_at
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
    df["ingested_at"] = pd.to_datetime(df["ingested_at"])
    return df


def latest_snapshot(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    latest_ts = df["snapshot_ts"].max()
    return df[df["snapshot_ts"] == latest_ts]


def within_range(df: pd.DataFrame, start_date: str | None, end_date: str | None) -> pd.DataFrame:
    if df.empty:
        return df
    if start_date:
        df = df[df["snapshot_ts"] >= pd.to_datetime(start_date)]
    if end_date:
        df = df[df["snapshot_ts"] < pd.to_datetime(end_date) + pd.Timedelta(days=1)]
    return df


def snapshot_at(df: pd.DataFrame, end_date: str | None) -> pd.DataFrame:
    """Latest snapshot on or before the range end."""
    return latest_snapshot(within_range(df, None, end_date))


def chip_coins(df: pd.DataFrame) -> pd.DataFrame:
    """Coins offered as chips, largest first, each with its fixed series color."""
    top = latest_snapshot(df).nlargest(CHIP_COUNT, "market_cap").copy()
    top["color"] = SERIES_COLORS[: len(top)]
    return top


def coin_colors(df: pd.DataFrame) -> dict[str, str]:
    chips = chip_coins(df)
    return dict(zip(chips["coin_id"], chips["color"]))


def weighted_change(df: pd.DataFrame) -> float | None:
    valid = df.dropna(subset=["price_change_percentage_24h"])
    if valid.empty or valid["market_cap"].sum() == 0:
        return None
    return float(
        (valid["price_change_percentage_24h"] * valid["market_cap"]).sum() / valid["market_cap"].sum()
    )


def format_usd(value: float) -> str:
    if value >= 1e12:
        return f"${value / 1e12:.2f}T"
    if value >= 1e9:
        return f"${value / 1e9:.0f}B"
    if value >= 1e6:
        return f"${value / 1e6:.0f}M"
    if value >= 1e3:
        return f"${value / 1e3:.1f}K"
    if value >= 1:
        return f"${value:,.2f}"
    return f"${value:.4g}"


def format_pct(pct: float | None, decimals: int = 1) -> str:
    if pct is None or pd.isna(pct):
        return "n/a"
    if round(pct, decimals) == 0:
        return f"{0:.{decimals}f}%"
    return f"{pct:+.{decimals}f}%"


def change_flag(pct: float | None) -> str:
    if pct is None or pd.isna(pct):
        return "unknown"
    if round(pct, 1) > 0:
        return FLAG_UP
    if round(pct, 1) < 0:
        return FLAG_DOWN
    return FLAG_FLAT


def change_badge(pct: float | None, *, boxed: bool = False) -> html.Span:
    """Signed % change with a direction glyph, so it never relies on color alone."""
    flag = change_flag(pct)
    glyph = {FLAG_UP: "▲ ", FLAG_DOWN: "▼ "}.get(flag, "")
    return html.Span(
        glyph + format_pct(pct).lstrip("+-"),
        className=f"change change-{flag}" + (" change-boxed" if boxed else ""),
    )


def card(children, class_name: str = "") -> html.Div:
    return html.Div(className=f"card {class_name}".strip(), children=children)


def card_header(title: str, subtitle=None, aside=None) -> html.Div:
    return html.Div(
        className="card-header",
        children=[
            html.Div([
                html.H2(title),
                html.P(subtitle, className="card-subtitle") if subtitle is not None else None,
            ]),
            html.Div(aside, className="card-aside") if aside is not None else None,
        ],
    )


def empty_figure(message: str = "No data for this range") -> go.Figure:
    fig = go.Figure()
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        xaxis={"visible": False},
        yaxis={"visible": False},
        margin={"l": 0, "r": 0, "t": 0, "b": 0},
        annotations=[{
            "text": message, "showarrow": False, "xref": "paper", "yref": "paper",
            "x": 0.5, "y": 0.5, "font": {"color": TEXT_MUTED, "family": FONT_SANS, "size": 13},
        }],
    )
    return fig


app = Dash(
    __name__,
    external_stylesheets=[
        "https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600&"
        "family=JetBrains+Mono:wght@400;500&family=Space+Grotesk:wght@600;700&display=swap",
    ],
)
app.title = "Crypto Market Dashboard"
server = app.server  # exposed for gunicorn (see dashboard/Dockerfile)

app.layout = html.Div(
    className="app-container",
    children=[
        dcc.Interval(id="refresh-interval", interval=REFRESH_INTERVAL_MS, n_intervals=0),
        dcc.Store(id="history-store"),
        html.Header(
            className="app-header",
            children=[
                html.Div(
                    className="brand",
                    children=[
                        html.Img(src=app.get_asset_url("logo.svg"), className="brand-logo", alt=""),
                        html.Div([
                            html.H1("Crypto Market Dashboard"),
                            html.Div(
                                className="pipeline",
                                children=[
                                    html.Span("CoinGecko", className="pill"),
                                    html.Span("→", className="pipeline-arrow"),
                                    html.Span("Airflow", className="pill"),
                                    html.Span("→", className="pipeline-arrow"),
                                    html.Span("DuckDB", className="pill"),
                                    html.Span("Daily snapshots · auto-refresh", className="pipeline-note"),
                                ],
                            ),
                        ]),
                    ],
                ),
                html.Div(
                    className="header-actions",
                    children=[
                        html.Div(id="status-badge", className="status-badge"),
                        html.Button(
                            "↻", id="refresh-button", className="icon-button",
                            title="Reload data", n_clicks=0,
                        ),
                    ],
                ),
            ],
        ),
        html.Div(id="empty-state"),
        card(
            class_name="filter-bar",
            children=[
                html.Div(
                    className="filter-coins",
                    children=[
                        html.Span("Coins", className="filter-label"),
                        dcc.Checklist(id="coin-selector", className="coin-chips", inline=True),
                    ],
                ),
                dcc.DatePickerRange(
                    id="date-range-selector",
                    display_format="MMM D, YYYY",
                    className="date-range",
                ),
            ],
        ),
        html.Div(id="kpi-row", className="kpi-row"),
        html.Div(
            className="grid grid-wide",
            children=[
                card([
                    card_header(
                        "Relative performance",
                        "% change since the first snapshot in the range · one point per daily snapshot",
                        html.Span("Indexed to 0%", className="tag"),
                    ),
                    dcc.Graph(id="performance-chart", config={"displayModeBar": False},
                              className="performance-graph"),
                ]),
                card([
                    card_header("Watchlist", aside=html.Span(id="watchlist-date")),
                    html.Div(
                        className="watchlist-head",
                        children=[html.Span("Coin"), html.Span("Trend"), html.Span("Mkt cap")],
                    ),
                    html.Div(id="watchlist"),
                ]),
            ],
        ),
        html.Div(
            className="grid grid-even",
            children=[
                card([
                    card_header("Top movers", aside=html.Span(id="movers-date")),
                    html.Div(id="top-movers", className="movers"),
                ]),
                card([
                    card_header(
                        "Market cap map",
                        html.Span(id="treemap-date"),
                        aside=html.Div(
                            className="scale-legend",
                            children=[
                                html.Span(f"-{TREEMAP_COLOR_LIMIT:.0f}%"),
                                html.Span(className="scale-bar"),
                                html.Span(f"+{TREEMAP_COLOR_LIMIT:.0f}%"),
                            ],
                        ),
                    ),
                    dcc.Graph(id="market-cap-treemap", config={"displayModeBar": False},
                              className="treemap-graph"),
                ]),
            ],
        ),
    ],
)


@app.callback(
    Output("history-store", "data"),
    Input("refresh-interval", "n_intervals"),
    Input("refresh-button", "n_clicks"),
)
def refresh_history_store(_n, _clicks):
    df = load_history()
    if df.empty:
        return None
    return df.to_json(date_format="iso", orient="split")


@app.callback(
    Output("status-badge", "children"),
    Output("empty-state", "children"),
    Input("history-store", "data"),
)
def refresh_status(data):
    df = history_from_store(data)
    if df.empty:
        return (
            [html.Span(className="status-dot status-stale"), "Waiting for first run"],
            html.Div(
                "No data yet — waiting for the Airflow pipeline's first successful run.",
                className="empty-state",
            ),
        )
    latest_ts = df["snapshot_ts"].max()
    ingested = df.loc[df["snapshot_ts"] == latest_ts, "ingested_at"].max()
    # The DAG runs daily, so a snapshot older than yesterday means a missed run.
    stale = latest_ts.normalize() < pd.Timestamp.now().normalize() - pd.Timedelta(days=1)
    label = f"Updated {ingested:%b %d, %Y, %H:%M} UTC"
    return (
        [
            html.Span(className="status-dot " + ("status-stale" if stale else "status-fresh")),
            ("Stale · " if stale else "") + label,
        ],
        "",
    )


@app.callback(
    Output("coin-selector", "options"),
    Output("coin-selector", "value"),
    Input("history-store", "data"),
    State("coin-selector", "value"),
)
def refresh_coin_options(data, current_value):
    df = history_from_store(data)
    if df.empty:
        return [], []
    chips = chip_coins(df)
    options = [
        {
            "label": html.Span(
                className="chip",
                style={"--chip-color": row["color"]},
                title=row["name"],
                children=[html.Span(className="chip-dot"), row["symbol"].upper()],
            ),
            "value": row["coin_id"],
        }
        for _, row in chips.iterrows()
    ]
    # Keep the viewer's selection across auto-refreshes.
    kept = [coin for coin in (current_value or []) if coin in set(chips["coin_id"])]
    return options, kept or chips["coin_id"].head(DEFAULT_SELECTED).tolist()


@app.callback(
    Output("date-range-selector", "min_date_allowed"),
    Output("date-range-selector", "max_date_allowed"),
    Output("date-range-selector", "start_date"),
    Output("date-range-selector", "end_date"),
    Input("history-store", "data"),
    State("date-range-selector", "start_date"),
    State("date-range-selector", "end_date"),
    State("date-range-selector", "max_date_allowed"),
)
def refresh_date_range(data, current_start, current_end, previous_max):
    df = history_from_store(data)
    if df.empty:
        return None, None, None, None
    min_date = df["snapshot_ts"].min().date()
    max_date = df["snapshot_ts"].max().date()
    start = current_start or max(min_date, max_date - pd.Timedelta(days=DEFAULT_RANGE_DAYS))
    # A range ending at the newest snapshot keeps following new snapshots.
    following_latest = current_end is None or (previous_max and current_end >= previous_max)
    end = max_date if following_latest else current_end
    return min_date, max_date, start, end


@app.callback(
    Output("kpi-row", "children"),
    Input("history-store", "data"),
    Input("coin-selector", "value"),
    Input("date-range-selector", "end_date"),
)
def update_kpis(data, selected_coins, end_date):
    df = history_from_store(data)
    snapshot = snapshot_at(df, end_date)
    if not snapshot.empty:
        snapshot = snapshot[snapshot["coin_id"].isin(selected_coins or [])]
    if snapshot.empty:
        return [card([html.P("Market overview", className="kpi-label"),
                      html.Div("—", className="kpi-value"),
                      html.P(NO_COINS_MESSAGE if not selected_coins else "No data for this range",
                             className="muted")], "kpi")]

    snapshot = snapshot.sort_values("market_cap", ascending=False)
    colors = coin_colors(df)
    total_cap = snapshot["market_cap"].sum()
    leader = snapshot.iloc[0]
    counts = snapshot["price_change_percentage_24h"].apply(change_flag).value_counts()
    up, down, flat = (int(counts.get(flag, 0)) for flag in (FLAG_UP, FLAG_DOWN, FLAG_FLAT))
    sentiment, sentiment_flag = (
        ("Bullish", FLAG_UP) if up > down else ("Bearish", FLAG_DOWN) if down > up else ("Neutral", FLAG_FLAT)
    )

    dominance_segments = [
        html.Span(
            className="dominance-segment",
            title=f"{row['name']}: {row['market_cap'] / total_cap:.1%}",
            style={"flexGrow": float(row["market_cap"]),
                   "backgroundColor": colors.get(row["coin_id"], TEXT_MUTED)},
        )
        for _, row in snapshot.iterrows()
    ]

    return [
        card(class_name="kpi", children=[
            html.P(f"Total market cap ({len(snapshot)} coins)", className="kpi-label"),
            html.Div(format_usd(total_cap), className="kpi-value"),
            html.Div(className="kpi-meta", children=[
                change_badge(weighted_change(snapshot), boxed=True), "24h, cap-weighted",
            ]),
        ]),
        card(class_name="kpi", children=[
            html.P(f"{leader['name']} price", className="kpi-label"),
            html.Div(format_usd(leader["current_price"]), className="kpi-value"),
            html.Div(className="kpi-meta", children=[
                change_badge(leader["price_change_percentage_24h"], boxed=True),
                f"24h · {leader['snapshot_ts']:%b %d}",
            ]),
        ]),
        card(class_name="kpi", children=[
            html.P(f"{leader['symbol'].upper()} dominance", className="kpi-label"),
            html.Div(f"{leader['market_cap'] / total_cap:.1%}", className="kpi-value"),
            html.Div(dominance_segments, className="dominance-bar",
                     title="Share of selected coins' total market cap"),
        ]),
        card(class_name="kpi", children=[
            html.P("Market sentiment", className="kpi-label"),
            html.Div(sentiment, className=f"kpi-value change-{sentiment_flag}"),
            html.Div(className="kpi-meta sentiment-counts", children=[
                html.Span([html.B(up, className="change-up"), " up"]),
                html.Span([html.B(down, className="change-down"), " down"]),
                html.Span([html.B(flat), " flat"]),
            ]),
        ]),
    ]


def range_change(prices: pd.Series) -> float | None:
    """% change from the first to the last snapshot, or None with fewer than two."""
    if len(prices) < 2 or prices.iloc[0] == 0:
        return None
    return float((prices.iloc[-1] / prices.iloc[0] - 1) * 100)


def snapshot_labels(ts: pd.Series) -> pd.Series:
    """Axis labels for snapshot dates; the year only shows when the range spans more than one."""
    return ts.dt.strftime("%b %d" if ts.dt.year.nunique() == 1 else "%b %d, %Y")


def spread_labels(values: list[float], min_gap: float) -> list[float]:
    """Nudge end-of-line label positions apart so they don't overlap."""
    order = sorted(range(len(values)), key=lambda i: values[i])
    placed = list(values)
    for prev, cur in zip(order, order[1:]):
        placed[cur] = max(placed[cur], placed[prev] + min_gap)
    # Re-center the stack so labels stay near their lines.
    shift = (sum(placed) - sum(values)) / len(values) if values else 0
    return [p - shift for p in placed]


@app.callback(
    Output("performance-chart", "figure"),
    Input("history-store", "data"),
    Input("coin-selector", "value"),
    Input("date-range-selector", "start_date"),
    Input("date-range-selector", "end_date"),
)
def update_performance_chart(data, selected_coins, start_date, end_date):
    df = history_from_store(data)
    colors = coin_colors(df) if not df.empty else {}
    if not selected_coins:
        return empty_figure(NO_COINS_MESSAGE)
    df = within_range(df, start_date, end_date)
    if df.empty:
        return empty_figure()
    df = df[df["coin_id"].isin(selected_coins)].sort_values("snapshot_ts").copy()
    if df.empty:
        return empty_figure()
    first_price = df.groupby("coin_id")["current_price"].transform("first")
    df["indexed_pct"] = (df["current_price"] / first_price - 1) * 100
    # Category axis: one evenly spaced point per stored snapshot, so days the
    # pipeline didn't run are skipped instead of drawn as a smooth trend.
    df["snapshot_label"] = snapshot_labels(df["snapshot_ts"])
    categories = df.drop_duplicates("snapshot_ts")["snapshot_label"].tolist()
    # ~6 evenly spaced tick labels, always including the latest snapshot.
    tick_step = -(-len(categories) // 6)
    tick_labels = categories[::-1][::tick_step][::-1]

    fig = go.Figure()
    ends = []
    for coin_id in selected_coins:
        series = df[df["coin_id"] == coin_id]
        if series.empty:
            continue
        color = colors.get(coin_id, TEXT_MUTED)
        symbol = series["symbol"].iloc[0].upper()
        fig.add_trace(go.Scatter(
            x=series["snapshot_label"],
            y=series["indexed_pct"],
            name=symbol,
            mode="lines+markers" if len(series) == 1 else "lines",
            line={"color": color, "width": 2},
            marker={"size": 8, "color": color},
            hovertemplate=f"{symbol} %{{y:+.1f}}%<extra></extra>",
        ))
        last = series.iloc[-1]
        ends.append((last["snapshot_label"], last["indexed_pct"], symbol, color))
        fig.add_trace(go.Scatter(
            x=[last["snapshot_label"]], y=[last["indexed_pct"]], mode="markers",
            marker={"size": 8, "color": color, "line": {"color": "#141821", "width": 2}},
            showlegend=False, hoverinfo="skip",
        ))

    y_values = df["indexed_pct"]
    y_span = max(y_values.max(), 0) - min(y_values.min(), 0) or 1
    chart_height = 320
    label_y = spread_labels([e[1] for e in ends], min_gap=y_span * 16 / (chart_height - 60))
    for (x, y, symbol, _color), ly in zip(ends, label_y):
        fig.add_annotation(
            x=x, y=ly, text=f"<b>{symbol}</b> {format_pct(y)}", showarrow=False,
            xanchor="left", xshift=10, font={"color": TEXT_SECONDARY, "family": FONT_MONO, "size": 11},
        )

    fig.update_layout(
        height=chart_height,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font={"family": FONT_SANS, "color": TEXT_SECONDARY, "size": 11},
        margin={"l": 44, "r": 110, "t": 8, "b": 32},
        showlegend=False,
        hovermode="x unified",
        hoverlabel={"bgcolor": "#1b1f2a", "bordercolor": GRID_COLOR,
                    "font": {"family": FONT_MONO, "color": TEXT_PRIMARY, "size": 11}},
        xaxis={"type": "category", "categoryorder": "array", "categoryarray": categories,
               "tickmode": "array", "tickvals": tick_labels, "tickangle": 0,
               "showgrid": False, "color": TEXT_MUTED,
               "tickfont": {"family": FONT_MONO}, "showspikes": True, "spikecolor": TEXT_MUTED,
               "spikethickness": 1, "spikedash": "dot", "spikemode": "across"},
        yaxis={"gridcolor": GRID_COLOR, "zeroline": True, "zerolinecolor": "#2a3040",
               "ticksuffix": "%", "color": TEXT_MUTED, "tickfont": {"family": FONT_MONO}},
    )
    return fig


def sparkline(series: pd.DataFrame, change: float | None) -> dcc.Graph:
    prices = series["current_price"]
    flag = change_flag(change) if change is not None else FLAG_FLAT
    fig = go.Figure(go.Scatter(
        x=list(range(len(prices))), y=prices, mode="lines+markers" if len(prices) == 1 else "lines",
        marker={"size": 4},
        line={"color": PRICE_CHANGE_COLORS[flag], "width": 1.5},
    ))
    fig.update_layout(
        height=36, margin={"l": 0, "r": 0, "t": 2, "b": 2},
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        xaxis={"visible": False}, showlegend=False,
        # At least ±1% of the mean price, so a stablecoin's tiny wobble reads as flat.
        yaxis={"visible": False, "range": [min(prices.min(), prices.mean() * 0.99),
                                           max(prices.max(), prices.mean() * 1.01)]},
    )
    return dcc.Graph(figure=fig, config={"staticPlot": True}, className="sparkline")


@app.callback(
    Output("watchlist", "children"),
    Output("watchlist-date", "children"),
    Input("history-store", "data"),
    Input("coin-selector", "value"),
    Input("date-range-selector", "start_date"),
    Input("date-range-selector", "end_date"),
)
def update_watchlist(data, selected_coins, start_date, end_date):
    df = history_from_store(data)
    colors = coin_colors(df) if not df.empty else {}
    in_range = within_range(df, start_date, end_date)
    snapshot = latest_snapshot(in_range)
    if not selected_coins:
        return html.P(NO_COINS_MESSAGE + ".", className="muted"), ""
    if snapshot.empty:
        return html.P("No data for this range.", className="muted"), ""
    snapshot = snapshot[snapshot["coin_id"].isin(selected_coins)].sort_values("market_cap", ascending=False)
    rows = []
    for _, row in snapshot.iterrows():
        color = colors.get(row["coin_id"], TEXT_MUTED)
        history = in_range[in_range["coin_id"] == row["coin_id"]].sort_values("snapshot_ts")
        change = range_change(history["current_price"])
        rows.append(html.Div(
            className="watch-row",
            children=[
                html.Div(className="watch-coin", children=[
                    html.Span(row["symbol"][:1].upper(), className="avatar",
                              style={"backgroundColor": color}),
                    html.Div([
                        html.Div(row["name"], className="watch-name"),
                        change_badge(change),
                    ]),
                ]),
                sparkline(history, change),
                html.Div(format_usd(row["market_cap"]), className="watch-cap"),
            ],
        ))
    first, last = in_range["snapshot_ts"].min(), in_range["snapshot_ts"].max()
    period = f"{first:%b %d}" if first == last else f"{first:%b %d} → {last:%b %d}"
    return rows, f"{period} · {in_range['snapshot_ts'].nunique()} snapshots"


def mover_rows(rows: pd.DataFrame, max_abs: float) -> list[html.Div]:
    return [
        html.Div(
            className="mover",
            title=f"{row['name']} ({row['symbol'].upper()}): {format_pct(row['price_change_percentage_24h'])}",
            children=[
                html.Div(className="mover-line", children=[
                    html.Span(row["name"], className="mover-name"),
                    html.Span(format_pct(row["price_change_percentage_24h"], 0),
                              className=f"mover-pct change-{row['price_change_flag']}"),
                ]),
                html.Div(className="mover-track", children=html.Div(
                    className=f"mover-bar bar-{row['price_change_flag']}",
                    style={"width": f"{abs(row['price_change_percentage_24h']) / max_abs * 100:.1f}%"},
                )),
            ],
        )
        for _, row in rows.iterrows()
    ]


@app.callback(
    Output("top-movers", "children"),
    Output("movers-date", "children"),
    Input("history-store", "data"),
    Input("date-range-selector", "end_date"),
)
def update_top_movers(data, end_date):
    snapshot = snapshot_at(history_from_store(data), end_date)
    if snapshot.empty:
        return html.P("No data for this range.", className="muted"), ""
    valid = snapshot.dropna(subset=["price_change_percentage_24h"])
    gainers = valid[valid["price_change_percentage_24h"] > 0].nlargest(TOP_MOVERS_COUNT, "price_change_percentage_24h")
    losers = valid[valid["price_change_percentage_24h"] < 0].nsmallest(TOP_MOVERS_COUNT, "price_change_percentage_24h")
    max_abs = pd.concat([gainers, losers])["price_change_percentage_24h"].abs().max() or 1
    return (
        [
            html.Div([html.H3("↗ Gainers", className="movers-title change-up"),
                      *mover_rows(gainers, max_abs)]),
            html.Div([html.H3("↘ Losers", className="movers-title change-down"),
                      *mover_rows(losers, max_abs)]),
        ],
        f"24h · {snapshot['snapshot_ts'].max():%b %d, %Y} · top {len(snapshot)}",
    )


# Diverging scale for the market cap map: red pole, neutral slate midpoint at
# 0%, green pole. Computed here rather than via a Plotly colorscale so the
# implicit treemap root doesn't get painted too.
DIVERGING_STOPS = ("#b8323f", "#2a3040", "#1f8a5b")


def diverging_color(pct: float) -> str:
    t = max(-1.0, min(1.0, pct / TREEMAP_COLOR_LIMIT))
    start, end = (DIVERGING_STOPS[1], DIVERGING_STOPS[2]) if t >= 0 else (DIVERGING_STOPS[1], DIVERGING_STOPS[0])
    a = [int(start[i:i + 2], 16) for i in (1, 3, 5)]
    b = [int(end[i:i + 2], 16) for i in (1, 3, 5)]
    return "#" + "".join(f"{round(x + (y - x) * abs(t)):02x}" for x, y in zip(a, b))


@app.callback(
    Output("market-cap-treemap", "figure"),
    Output("treemap-date", "children"),
    Input("history-store", "data"),
    Input("coin-selector", "value"),
    Input("date-range-selector", "end_date"),
)
def update_market_cap_treemap(data, selected_coins, end_date):
    if not selected_coins:
        return empty_figure(NO_COINS_MESSAGE), ""
    snapshot = snapshot_at(history_from_store(data), end_date)
    if not snapshot.empty:
        snapshot = snapshot[snapshot["coin_id"].isin(selected_coins)]
    if snapshot.empty:
        return empty_figure(), ""
    snapshot = snapshot.sort_values("market_cap", ascending=False)
    # Point-in-time view: only the range end matters, so say which snapshot it is.
    caption = f"{snapshot['snapshot_ts'].max():%b %d, %Y}"
    change = snapshot["price_change_percentage_24h"].fillna(0)
    fig = go.Figure(go.Treemap(
        labels=snapshot["name"],
        parents=[""] * len(snapshot),
        values=snapshot["market_cap"],
        customdata=list(zip(
            snapshot["symbol"].str.upper(),
            snapshot["market_cap"].apply(format_usd),
            snapshot["price_change_percentage_24h"].apply(format_pct),
        )),
        texttemplate="<b>%{label}</b><br>%{customdata[0]}<br><br>%{customdata[1]} · %{customdata[2]}",
        hovertemplate="<b>%{label}</b> (%{customdata[0]})<br>%{customdata[1]} · %{customdata[2]}<extra></extra>",
        textposition="top left",
        textfont={"family": FONT_SANS, "color": TEXT_PRIMARY, "size": 13},
        marker={
            "colors": change.apply(diverging_color),
            "cornerradius": 6,
            "line": {"color": "#141821", "width": 2},
        },
        tiling={"pad": 2},
        root={"color": "#141821"},
        pathbar={"visible": False},
        sort=True,
    ))
    fig.update_layout(
        height=330,
        paper_bgcolor="rgba(0,0,0,0)",
        margin={"l": 0, "r": 0, "t": 0, "b": 0},
        hoverlabel={"bgcolor": "#1b1f2a", "bordercolor": GRID_COLOR,
                    "font": {"family": FONT_MONO, "color": TEXT_PRIMARY, "size": 11}},
    )
    return fig, caption


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8050, debug=False)
