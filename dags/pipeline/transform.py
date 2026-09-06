"""Cleaning / shaping logic for raw CoinGecko payloads."""
from __future__ import annotations

import os
from pathlib import Path

import pandas as pd

from constants import FLAG_DOWN, FLAG_FLAT, FLAG_UNKNOWN, FLAG_UP

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


def _price_change_flag(pct: float | None) -> str:
    if pct is None or pd.isna(pct):
        return FLAG_UNKNOWN
    if pct > 0:
        return FLAG_UP
    if pct < 0:
        return FLAG_DOWN
    return FLAG_FLAT


def transform_market_data(raw: list[dict], snapshot_ts: str) -> pd.DataFrame:
    """Clean, validate types, dedupe, and add derived fields.

    `snapshot_ts` is the DAG's logical date (Airflow `ds`), used as the
    idempotency key when loading into DuckDB.
    """
    if not raw:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    df = pd.DataFrame(raw)

    # Drop rows missing any field essential to identifying/pricing a coin.
    present_required = [c for c in REQUIRED_RAW_FIELDS if c in df.columns]
    df = df.dropna(subset=present_required)

    # Some fields are optional in the API response.
    if "price_change_percentage_24h" not in df.columns:
        df["price_change_percentage_24h"] = pd.NA

    df = df.rename(columns={"id": "coin_id"})

    df["coin_id"] = df["coin_id"].astype(str)
    df["symbol"] = df["symbol"].astype(str).str.lower()
    df["name"] = df["name"].astype(str)
    df["current_price"] = pd.to_numeric(df["current_price"], errors="coerce")
    df["market_cap"] = pd.to_numeric(df["market_cap"], errors="coerce").astype("Int64")
    df["market_cap_rank"] = pd.to_numeric(df["market_cap_rank"], errors="coerce").astype("Int64")
    df["total_volume"] = pd.to_numeric(df["total_volume"], errors="coerce").astype("Int64")
    df["price_change_percentage_24h"] = pd.to_numeric(
        df["price_change_percentage_24h"], errors="coerce"
    )

    # Drop rows where required numeric casts failed.
    df = df.dropna(subset=["current_price", "market_cap"])

    df["price_change_flag"] = df["price_change_percentage_24h"].apply(_price_change_flag)
    df["snapshot_ts"] = snapshot_ts

    df = df.drop_duplicates(subset=["coin_id"], keep="first")

    return df[OUTPUT_COLUMNS].reset_index(drop=True)


def save_processed(df: pd.DataFrame, out_path: str | os.PathLike) -> str:
    path = Path(out_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)
    return str(path)
