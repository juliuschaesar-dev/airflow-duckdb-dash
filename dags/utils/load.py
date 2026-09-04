"""Idempotent load into DuckDB."""
from __future__ import annotations

import os
from pathlib import Path

import duckdb
import pandas as pd

from .transform import OUTPUT_COLUMNS

TABLE_NAME = "crypto_market_data"

# SQL type for each column in OUTPUT_COLUMNS — the single source of truth
# for the table schema, so it can't drift out of sync with the transform step.
_COLUMN_TYPES = {
    "coin_id": "VARCHAR NOT NULL",
    "symbol": "VARCHAR",
    "name": "VARCHAR",
    "current_price": "DOUBLE",
    "market_cap": "BIGINT",
    "market_cap_rank": "INTEGER",
    "total_volume": "BIGINT",
    "price_change_percentage_24h": "DOUBLE",
    "price_change_flag": "VARCHAR",
    "snapshot_ts": "VARCHAR NOT NULL",
}

_column_defs = ",\n    ".join(f"{col} {_COLUMN_TYPES[col]}" for col in OUTPUT_COLUMNS)
CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    {_column_defs},
    ingested_at TIMESTAMP NOT NULL DEFAULT current_timestamp,
    PRIMARY KEY (coin_id, snapshot_ts)
);
"""

_insert_columns_sql = ", ".join(OUTPUT_COLUMNS)
# snapshot_ts is bound from the function argument rather than read off the
# incoming DataFrame, so a stale/mismatched value in `df` can't get loaded.
_select_exprs_sql = ", ".join("?" if col == "snapshot_ts" else col for col in OUTPUT_COLUMNS)
UPSERT_SQL = f"""
INSERT INTO {TABLE_NAME} ({_insert_columns_sql})
SELECT {_select_exprs_sql}
FROM new_data
"""

DELETE_SNAPSHOT_SQL = f"DELETE FROM {TABLE_NAME} WHERE snapshot_ts = ?"


def load_market_data(df: pd.DataFrame, db_path: str | os.PathLike, snapshot_ts: str) -> int:
    """Upsert `df` into DuckDB, keyed on (coin_id, snapshot_ts).

    Re-running the same DAG run (same logical date) replaces that run's
    rows instead of duplicating them, making the load idempotent.
    """
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(db_path), read_only=False)
    try:
        con.execute(CREATE_TABLE_SQL)
        con.register("new_data", df)

        con.execute(DELETE_SNAPSHOT_SQL, [snapshot_ts])
        con.execute(UPSERT_SQL, [snapshot_ts])
        inserted = con.execute(
            f"SELECT count(*) FROM {TABLE_NAME} WHERE snapshot_ts = ?", [snapshot_ts]
        ).fetchone()[0]
        return int(inserted)
    finally:
        con.close()
