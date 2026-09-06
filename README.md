# crypto-airflow-duckdb-dash

End-to-end crypto market data pipeline: **CoinGecko API → Airflow → DuckDB → Plotly Dash**.

## Architecture

<img src="docs/architecture.svg" width="100%" alt="Pipeline architecture: CoinGecko API, Airflow (extract, transform_and_validate, load), DuckDB, Plotly Dash, PostgreSQL, Airflow UI, airflow-init">

- **Orchestration**: Apache Airflow 3.3.1 (LocalExecutor, Postgres metadata DB), Python 3.13
- **Extraction**: `requests`, with retry/backoff for CoinGecko's rate limits
- **Storage**: DuckDB, single embedded file at `data/crypto.duckdb`
- **Visualization**: Plotly Dash, its own container, reads DuckDB read-only
- **Tests**: pytest, unit tests for `dags/pipeline/` modules (`tests/pipeline/`)

## Repo structure

```
├── dags/
│   ├── crypto_pipeline.py      # DAG wiring (Airflow 3 Task SDK / TaskFlow API)
│   ├── constants.py            # shared literals: table name, schema columns, flags
│   ├── .airflowignore          # excludes pipeline/ and constants.py from DAG-file parsing
│   └── pipeline/               # extract/transform/validate/load — no Airflow dependency
├── plugins/                    # custom operators/hooks/sensors (none yet — placeholder)
├── dashboard/
│   ├── app.py
│   ├── assets/style.css
│   └── Dockerfile              # python:3.13.14-slim
├── docker/airflow/
│   └── Dockerfile              # apache/airflow:3.3.1-python3.13
├── docs/
│   └── architecture.svg        # diagram rendered at the top of this README
├── requirements/                # single source of truth for dependency floors
│   ├── common.txt               # duckdb, pandas — shared by every environment
│   ├── airflow.txt               # common + requests, pyarrow, apache-airflow-providers-fab
│   ├── test.txt                  # common + requests, pyarrow, pytest
│   └── dashboard.txt             # common + dash, plotly, gunicorn
├── data/                       # crypto.duckdb lives here at runtime (gitignored)
├── tests/
│   └── pipeline/                # business logic — no Airflow install required
├── .env.example                # copy to .env — see Configuration below
└── docker-compose.yml
```

Both Dockerfiles build from the repo root (not their own subdirectory) so they
can `COPY requirements/` and install the right file for their environment.

A few deliberate omissions from the common Airflow project template: no
`config/airflow.cfg` (everything is configured via `AIRFLOW__*` env vars in
`docker-compose.yml`, the recommended approach for containerized Airflow),
no host-mounted `logs/` (logs live on the `airflow_logs` Docker volume
instead, avoiding both repo clutter and host file-permission issues), and no
`include/` (that convention pays off when SQL is executed via an Airflow SQL
operator that renders Jinja using Airflow's own execution context — `load.py`
calls DuckDB directly from plain Python and only needs to substitute values
it already holds as Python objects, so building the SQL in Python keeps it
simpler and avoids a needless templating dependency).

## Configuration

All credentials and environment-specific config are read from a `.env` file
(gitignored, never committed) — `docker-compose.yml` only references
`${VARIABLE}` placeholders.

```bash
cp .env.example .env
```

| Variable | Purpose | Default |
| --- | --- | --- |
| `POSTGRES_USER` / `POSTGRES_PASSWORD` / `POSTGRES_DB` | Airflow's metadata database | `airflow` / `airflow` / `airflow` |
| `AIRFLOW_ADMIN_USERNAME` / `AIRFLOW_ADMIN_PASSWORD` | Airflow web UI login | `admin` / `admin` |
| `AIRFLOW_JWT_SECRET` / `AIRFLOW_JWT_ISSUER` | Shared secret Airflow components use to authenticate to `airflow-apiserver` | `airflow_jwt_secret` / `airflow` |
| `CRYPTO_API_BASE_URL` | CoinGecko `/coins/markets` endpoint the `extract` task calls | `https://api.coingecko.com/api/v3/coins/markets` |
| `CRYPTO_TOP_N` | Coins to pull per run, by market cap (CoinGecko's `per_page` cap is 250) | `250` |
| `CRYPTO_REFRESH_MS` | Dashboard auto-refresh interval, in milliseconds | `3600000` (60 min) |

## Running it

```bash
docker compose up --build -d
docker compose logs -f airflow-init  # watch db migrate + admin user creation
```

- Airflow UI (api-server): http://localhost:8080 (login: `AIRFLOW_ADMIN_USERNAME`
  / `AIRFLOW_ADMIN_PASSWORD` from `.env` — handled by the FAB auth manager,
  set explicitly since Airflow 3's new default `SimpleAuthManager` auto-generates
  a one-time password instead)
- Dash dashboard: http://localhost:8050

`-d` runs the stack detached, independent of the terminal session. To stop it:
`docker compose down` (add `-v` to also drop the Postgres/log volumes).

The `crypto_pipeline` DAG is unpaused on creation and scheduled `0 6 * * *`.
Trigger a manual run from the Airflow UI to see data show up on the dashboard
right away instead of waiting for the next scheduled slot.

## Pipeline design

1. **extract** — calls `GET /coins/markets` (top 250 coins by market cap, USD),
   writes the raw JSON response to `data/raw/{ds}.json`.
2. **transform_and_validate** (TaskGroup)
   - **transform** — casts types, drops rows missing required fields, dedupes
     by `coin_id`, derives `price_change_flag` (`up`/`down`/`flat`/`unknown`),
     writes `data/processed/{ds}.parquet`.
   - **quality_check** — asserts non-empty, no duplicate/null `coin_id`,
     no negative prices, all expected columns present. Raises
     `AirflowException` on failure so the DAG stops before touching DuckDB.
3. **load** — upserts into `crypto_market_data`, keyed on `(coin_id, snapshot_ts)`:
   deletes any existing rows for that snapshot, then inserts the new batch.
   Re-running the same logical date is idempotent — no duplicate rows. The
   `CREATE TABLE`/`INSERT` column list in `dags/pipeline/load.py` is generated
   from `constants.OUTPUT_COLUMNS` at import time, so the table schema can't
   drift out of sync with what `transform` actually produces.

Reliability: 3 retries with exponential backoff per task, `max_active_runs=1`
to avoid overlapping runs. Failed task instances already show up in the
Airflow UI; add an `on_failure_callback` to `default_args` in
`crypto_pipeline.py` if Slack/email/PagerDuty alerting is needed later.

## DuckDB single-writer note

DuckDB allows only one read-write connection at a time. Airflow's `load`
task is the only process that opens `data/crypto.duckdb` for writing;
the Dash service always connects with `read_only=True`, and Airflow runs
with `max_active_runs=1` so writes never overlap.

## Dashboard

- **Line chart** — price over time, one line per selected coin
- **Bar chart** — top 5 gainers and top 5 losers by 24h % change
- **Treemap** — market cap comparison across all coins, colored by 24h change

Refreshes automatically every 60 minutes via `dcc.Interval` (`CRYPTO_REFRESH_MS`).

## Tests

```bash
pip install -r requirements/test.txt
pytest tests/
```

- `tests/pipeline/` — one file per `dags/pipeline/` module (`test_transform.py`,
  `test_validate.py`, `test_load.py`), no Airflow install required.
