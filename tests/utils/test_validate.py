"""Unit tests for dags/utils/validate.py."""
from __future__ import annotations

import pandas as pd
import pytest

from utils.transform import transform_market_data
from utils.validate import DataQualityError, validate_market_data


class TestValidate:
    def test_passes_on_clean_data(self, raw_sample):
        df = transform_market_data(raw_sample, snapshot_ts="2024-01-01")
        validate_market_data(df)  # should not raise

    def test_fails_on_empty_dataframe(self):
        df = transform_market_data([], snapshot_ts="2024-01-01")
        with pytest.raises(DataQualityError):
            validate_market_data(df, min_rows=1)

    def test_fails_on_duplicate_coin_id(self, raw_sample):
        df = transform_market_data(raw_sample, snapshot_ts="2024-01-01")
        dupe_row = df.iloc[[0]]
        df_with_dupe = pd.concat([df, dupe_row], ignore_index=True)
        with pytest.raises(DataQualityError):
            validate_market_data(df_with_dupe)

    def test_fails_on_missing_column(self, raw_sample):
        df = transform_market_data(raw_sample, snapshot_ts="2024-01-01")
        df = df.drop(columns=["market_cap"])
        with pytest.raises(DataQualityError):
            validate_market_data(df)

    def test_fails_on_negative_price(self, raw_sample):
        df = transform_market_data(raw_sample, snapshot_ts="2024-01-01")
        df.loc[0, "current_price"] = -1
        with pytest.raises(DataQualityError):
            validate_market_data(df)
