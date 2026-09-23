"""Constants shared across the pipeline and the dashboard.

The dashboard runs in its own container with its own Dockerfile (it never
has `dags/` in its image), so `dashboard/Dockerfile` copies this single file
in alongside `app.py` rather than importing the `dags.pipeline` package.
"""
from __future__ import annotations

CRYPTO_MARKET_DATA = "crypto_market_data"

FLAG_UP = "up"
FLAG_DOWN = "down"
FLAG_FLAT = "flat"
FLAG_UNKNOWN = "unknown"
PRICE_CHANGE_FLAGS = (FLAG_UP, FLAG_DOWN, FLAG_FLAT, FLAG_UNKNOWN)

# Single source of truth for the dashboard's flag -> color mapping, so a new
# flag added above can't silently end up uncolored on the dashboard's trend
# sparklines. Tuned for the dashboard's dark surface.
PRICE_CHANGE_COLORS = {
    FLAG_UP: "#3ecf8e",
    FLAG_DOWN: "#f0616d",
    FLAG_FLAT: "#8b93a7",
    FLAG_UNKNOWN: "#5b6275",
}

# Raw CoinGecko fields a row must have to be kept.
REQUIRED_RAW_FIELDS = [
    "id",
    "symbol",
    "name",
    "current_price",
    "market_cap",
    "market_cap_rank",
    "total_volume",
]

# Columns the pipeline persists downstream, in a stable order.
OUTPUT_COLUMNS = [
    "coin_id",
    "symbol",
    "name",
    "current_price",
    "market_cap",
    "market_cap_rank",
    "total_volume",
    "price_change_percentage_24h",
    "price_change_flag",
    "snapshot_ts",
]
