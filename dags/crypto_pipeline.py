"""Extract → transform → validate → load pipeline for CoinGecko market data.

Business logic lives in `dags.utils` (extract/transform/validate/load) so it
can be unit-tested without Airflow installed; this module just wires those
functions into a DAG via the TaskFlow API.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta

import pandas as pd
from airflow.sdk import DAG, TaskGroup, task
from airflow.sdk.exceptions import AirflowException

from utils.extract import COINGECKO_MARKETS_URL, fetch_market_data, load_raw_response, save_raw_response
from utils.load import load_market_data
from utils.transform import save_processed, transform_market_data
from utils.validate import DataQualityError, validate_market_data

logger = logging.getLogger(__name__)

DATA_DIR = os.environ.get("CRYPTO_DATA_DIR", "/opt/airflow/data")
DB_PATH = os.path.join(DATA_DIR, "crypto.duckdb")
TOP_N_COINS = int(os.environ.get("CRYPTO_TOP_N", "50"))
API_BASE_URL = os.environ.get("CRYPTO_API_BASE_URL", COINGECKO_MARKETS_URL)


def _snapshot_ts(logical_date) -> str:
    """Hourly snapshot key, e.g. '2026-09-05T06' — `ds` alone would collapse
    all four runs of the same calendar day onto one snapshot."""
    return logical_date.strftime("%Y-%m-%dT%H")


def _snapshot_path(subdir: str, extension: str, snapshot_ts: str) -> str:
    return os.path.join(DATA_DIR, subdir, f"{snapshot_ts}.{extension}")


def alert_on_failure(context: dict) -> None:
    """Extension point for paging/Slack/email — logs today, wire up a real
    notifier (SlackWebhookOperator, EmailOperator, etc.) here later."""
    ti = context["task_instance"]
    logger.error(
        "Task %s in DAG %s failed on run %s",
        ti.task_id,
        ti.dag_id,
        context.get("logical_date"),
    )


default_args = {
    "owner": "data-eng",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "on_failure_callback": alert_on_failure,
}

with DAG(
    dag_id="crypto_pipeline",
    description="Extract crypto market data from CoinGecko and load it into DuckDB",
    default_args=default_args,
    schedule="0 */6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["crypto", "coingecko", "duckdb"],
) as dag:

    @task(task_id="extract")
    def extract_task(logical_date) -> str:
        snapshot_ts = _snapshot_ts(logical_date)
        raw = fetch_market_data(top_n=TOP_N_COINS, base_url=API_BASE_URL)
        return save_raw_response(raw, _snapshot_path("raw", "json", snapshot_ts))

    @task(task_id="transform")
    def transform_task(raw_path: str, logical_date) -> str:
        snapshot_ts = _snapshot_ts(logical_date)
        raw = load_raw_response(raw_path)
        df = transform_market_data(raw, snapshot_ts=snapshot_ts)
        return save_processed(df, _snapshot_path("processed", "parquet", snapshot_ts))

    @task(task_id="quality_check")
    def quality_check_task(processed_path: str) -> str:
        df = pd.read_parquet(processed_path)
        try:
            validate_market_data(df, min_rows=1)
        except DataQualityError as exc:
            raise AirflowException(f"Data quality check failed: {exc}") from exc
        return processed_path

    @task(task_id="load")
    def load_task(processed_path: str, logical_date) -> int:
        snapshot_ts = _snapshot_ts(logical_date)
        df = pd.read_parquet(processed_path)
        rows = load_market_data(df, DB_PATH, snapshot_ts=snapshot_ts)
        logger.info("Loaded %s rows into %s for snapshot_ts=%s", rows, DB_PATH, snapshot_ts)
        return rows

    raw_path = extract_task()

    with TaskGroup(group_id="transform_and_validate"):
        transformed_path = transform_task(raw_path=raw_path)
        validated_path = quality_check_task(processed_path=transformed_path)

    load_task(processed_path=validated_path)
