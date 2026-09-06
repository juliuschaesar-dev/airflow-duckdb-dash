"""Data quality gate that runs between transform and load."""
from __future__ import annotations

import pandas as pd

from constants import PRICE_CHANGE_FLAGS
from .transform import OUTPUT_COLUMNS


class DataQualityError(ValueError):
    """Raised when the transformed batch fails a quality check."""


def validate_market_data(df: pd.DataFrame, min_rows: int = 1) -> None:
    missing_cols = [c for c in OUTPUT_COLUMNS if c not in df.columns]
    if missing_cols:
        raise DataQualityError(f"Missing expected columns: {missing_cols}")

    if len(df) < min_rows:
        raise DataQualityError(f"Expected at least {min_rows} row(s), got {len(df)}")

    if df["coin_id"].duplicated().any():
        dupes = df.loc[df["coin_id"].duplicated(), "coin_id"].tolist()
        raise DataQualityError(f"Duplicate coin_id values found: {dupes}")

    if df["coin_id"].isna().any() or (df["coin_id"] == "").any():
        raise DataQualityError("Null or empty coin_id values found")

    if (df["current_price"] < 0).any():
        raise DataQualityError("Negative current_price values found")

    if (df["market_cap"] < 0).any():
        raise DataQualityError("Negative market_cap values found")

    bad_flags = set(df["price_change_flag"].unique()) - set(PRICE_CHANGE_FLAGS)
    if bad_flags:
        raise DataQualityError(f"Unexpected price_change_flag values: {bad_flags}")
