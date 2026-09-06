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
