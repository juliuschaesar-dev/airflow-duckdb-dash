"""Unit tests for dags/pipeline/transform.py."""
from __future__ import annotations

from pipeline.transform import OUTPUT_COLUMNS, transform_market_data


class TestTransform:
    def test_output_shape(self, raw_sample):
        df = transform_market_data(raw_sample, snapshot_ts="2024-01-01")
        assert list(df.columns) == OUTPUT_COLUMNS

    def test_dedupes_by_coin_id(self, raw_sample):
        df = transform_market_data(raw_sample, snapshot_ts="2024-01-01")
        assert df["coin_id"].tolist().count("bitcoin") == 1

    def test_drops_rows_missing_required_fields(self, raw_sample):
        df = transform_market_data(raw_sample, snapshot_ts="2024-01-01")
        assert "broken-coin" not in df["coin_id"].tolist()

    def test_price_change_flag(self, raw_sample):
        df = transform_market_data(raw_sample, snapshot_ts="2024-01-01")
        flags = dict(zip(df["coin_id"], df["price_change_flag"]))
        assert flags["bitcoin"] == "up"
        assert flags["ethereum"] == "down"

    def test_flat_and_unknown_flags(self, raw_sample):
        raw = [
            {**raw_sample[0], "id": "flat-coin", "price_change_percentage_24h": 0.0},
            {**raw_sample[0], "id": "unknown-coin"},
        ]
        del raw[1]["price_change_percentage_24h"]
        df = transform_market_data(raw, snapshot_ts="2024-01-01")
        flags = dict(zip(df["coin_id"], df["price_change_flag"]))
        assert flags["flat-coin"] == "flat"
        assert flags["unknown-coin"] == "unknown"

    def test_empty_input_returns_empty_frame_with_correct_columns(self):
        df = transform_market_data([], snapshot_ts="2024-01-01")
        assert df.empty
        assert list(df.columns) == OUTPUT_COLUMNS

    def test_snapshot_ts_stamped_on_every_row(self, raw_sample):
        df = transform_market_data(raw_sample, snapshot_ts="2024-06-15")
        assert (df["snapshot_ts"] == "2024-06-15").all()
