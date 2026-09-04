"""CoinGecko extraction helpers — no Airflow dependency so it stays unit-testable."""
from __future__ import annotations

import json
import os
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

COINGECKO_MARKETS_URL = "https://api.coingecko.com/api/v3/coins/markets"


def build_session(total_retries: int = 5, backoff_factor: float = 2.0) -> requests.Session:
    """CoinGecko's free tier rate-limits aggressively; back off on 429/5xx."""
    session = requests.Session()
    retry = Retry(
        total=total_retries,
        backoff_factor=backoff_factor,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def fetch_market_data(
    vs_currency: str = "usd",
    top_n: int = 50,
    session: requests.Session | None = None,
    timeout: int = 30,
    base_url: str = COINGECKO_MARKETS_URL,
) -> list[dict]:
    session = session or build_session()
    params = {
        "vs_currency": vs_currency,
        "order": "market_cap_desc",
        "per_page": top_n,
        "page": 1,
        "price_change_percentage": "24h",
        "sparkline": "false",
    }
    response = session.get(base_url, params=params, timeout=timeout)
    response.raise_for_status()
    return response.json()


def save_raw_response(payload: list[dict], out_path: str | os.PathLike) -> str:
    path = Path(out_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return str(path)


def load_raw_response(path: str | os.PathLike) -> list[dict]:
    return json.loads(Path(path).read_text(encoding="utf-8"))
