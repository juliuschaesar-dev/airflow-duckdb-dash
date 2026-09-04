"""DAG integrity checks: imports cleanly, structure matches expectations.

Business logic itself is covered under tests/unit/ — these tests only care
that the DAG wires together correctly. Skipped when Airflow isn't installed
(e.g. running `pytest` outside the Airflow container).
"""
from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest

DAGS_DIR = Path(__file__).resolve().parents[2] / "dags"


@pytest.fixture(scope="module")
def dag():
    pytest.importorskip("airflow")
    sys.path.insert(0, str(DAGS_DIR))
    module = importlib.import_module("crypto_pipeline")
    return module.dag


def test_expected_tasks_present(dag):
    assert set(dag.task_ids) == {
        "extract",
        "transform_and_validate.transform",
        "transform_and_validate.quality_check",
        "load",
    }


def test_dag_metadata(dag):
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    assert dag.tags == {"crypto", "coingecko", "duckdb"}


def test_task_retry_policy(dag):
    for task in dag.tasks:
        assert task.retries == 3
