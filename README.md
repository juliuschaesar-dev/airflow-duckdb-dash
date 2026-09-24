# crypto-airflow-duckdb-dash

End-to-end crypto market data pipeline: **CoinGecko API → Airflow → DuckDB → Plotly Dash**.

## Architecture

<img src="docs/architecture.svg" width="100%" alt="Pipeline architecture: CoinGecko API, Airflow (extract, transform_and_validate, load), DuckDB, Plotly Dash, PostgreSQL, Airflow UI, airflow-init">

- **Orchestration**: Apache Airflow 3.3.2 (LocalExecutor, Postgres metadata DB), Python 3.14
- **Extraction**: `requests`, with retry/backoff for CoinGecko's rate limits
- **Storage**: DuckDB, single embedded file at `data/crypto.duckdb`
- **Visualization**: Plotly Dash, its own container, reads DuckDB read-only
- **Tests**: pytest, unit tests for `dags/pipeline/` modules (`tests/pipeline/`)

## Repo structure

```
├── dags/                        # DAG wiring, shared constants, extract/transform/validate/load pipeline
├── plugins/                     # custom operators/hooks/sensors (none yet — placeholder)
├── dashboard/                   # Plotly Dash app
├── docker/airflow/              # Airflow image build
├── docs/                        # architecture diagram, screenshots
├── requirements/                # single source of truth for dependency floors
├── data/                        # crypto.duckdb + raw/processed snapshots (bind-mounted into Docker)
├── tests/                       # business logic tests — no Airflow install required
├── .env.example                 # copy to .env — see Configuration below
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

`-d` runs the stack detached, independent of the terminal session.

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

`data/` is bind-mounted into the containers, so the database lives on the
host. Open it read-only from host tools, and not while `load` is running.

## Dashboard

Dashboard screenshot for reference:

![Dashboard - KPI cards, relative performance chart and watchlist](docs/screenshots/Crypto%20Market%20Dashboard-1.png)
![Dashboard - top movers and market cap map](docs/screenshots/Crypto%20Market%20Dashboard-2.png)

Refreshes automatically every 10 minutes via `dcc.Interval` (`CRYPTO_REFRESH_MS`).

After changing `dashboard/app.py` or its assets, rebuild just this service:

```bash
docker compose up --build -d dashboard
```

## Tests

```bash
pip install -r requirements/test.txt
pytest tests/
```

- `tests/pipeline/` — one file per `dags/pipeline/` module (`test_transform.py`,
  `test_validate.py`, `test_load.py`), no Airflow install required.

## Stopping

```bash
docker compose down
```

Use `docker compose down -v` to also remove the Postgres metadata DB and
Airflow logs volumes. The DuckDB database and snapshots in `data/` are on the
host, so they are kept; delete `data/crypto.duckdb` to start from an empty
database.
