"""Unit tests for dags/pipeline/load.py."""
from __future__ import annotations

import duckdb

from pipeline.load import CRYPTO_MARKET_DATA, load_market_data
from pipeline.transform import transform_market_data


class TestLoad:
    def test_load_is_idempotent(self, raw_sample, tmp_path):
        db_path = tmp_path / "test.duckdb"
        df = transform_market_data(raw_sample, snapshot_ts="2024-01-01")

        rows_first = load_market_data(df, db_path, snapshot_ts="2024-01-01")
        rows_second = load_market_data(df, db_path, snapshot_ts="2024-01-01")

        assert rows_first == rows_second == len(df)

        con = duckdb.connect(str(db_path), read_only=True)
        try:
            total = con.execute(f"SELECT count(*) FROM {CRYPTO_MARKET_DATA}").fetchone()[0]
        finally:
            con.close()
        assert total == len(df)

    def test_load_keeps_separate_snapshots(self, raw_sample, tmp_path):
        db_path = tmp_path / "test.duckdb"
        df = transform_market_data(raw_sample, snapshot_ts="2024-01-01")

        load_market_data(df, db_path, snapshot_ts="2024-01-01")
        load_market_data(df, db_path, snapshot_ts="2024-01-02")

        con = duckdb.connect(str(db_path), read_only=True)
        try:
            total = con.execute(f"SELECT count(*) FROM {CRYPTO_MARKET_DATA}").fetchone()[0]
        finally:
            con.close()
        assert total == 2 * len(df)
