"""Shared fixtures for dags/pipeline unit tests.

Puts `dags/` on sys.path so tests can `import pipeline.xxx` the same way
`dags/crypto_pipeline.py` does, without requiring Airflow to be installed.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "dags"))


@pytest.fixture
def raw_sample() -> list[dict]:
    return [
        {
            "id": "bitcoin",
            "symbol": "btc",
            "name": "Bitcoin",
            "current_price": 65000.5,
            "market_cap": 1_280_000_000_000,
            "market_cap_rank": 1,
            "total_volume": 30_000_000_000,
            "price_change_percentage_24h": 2.5,
        },
        {
            "id": "ethereum",
            "symbol": "eth",
            "name": "Ethereum",
            "current_price": 3500.0,
            "market_cap": 420_000_000_000,
            "market_cap_rank": 2,
            "total_volume": 15_000_000_000,
            "price_change_percentage_24h": -1.2,
        },
        # duplicate of bitcoin, should be deduped
        {
            "id": "bitcoin",
            "symbol": "btc",
            "name": "Bitcoin",
            "current_price": 65010.0,
            "market_cap": 1_280_500_000_000,
            "market_cap_rank": 1,
            "total_volume": 30_000_000_000,
            "price_change_percentage_24h": 2.6,
        },
        # missing current_price, should be dropped
        {
            "id": "broken-coin",
            "symbol": "brk",
            "name": "Broken Coin",
            "current_price": None,
            "market_cap": 1_000_000,
            "market_cap_rank": 999,
            "total_volume": 1000,
            "price_change_percentage_24h": 0.0,
        },
    ]
