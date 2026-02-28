# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Data pipeline for ingesting and managing Polygon.io US stocks and options market data. Downloads flat files from Polygon's S3, converts CSV.gz to hive-partitioned Parquet, fetches missing options contract metadata from the Polygon REST API, and exposes everything through DuckDB in-memory views.

Multi-developer (Jim on Linux, Dan on Windows). Each developer creates their own `config.py` from `config_sample.py` (git-ignored, contains API key and local paths).

## Running

No build system or test framework. `PYTHONPATH=.` is set via `.env` (git-ignored). Requires `rclone` configured with a remote named `s3polygon` pointing at the Polygon.io S3 flat file bucket. Scripts are run directly:

```bash
python download.py      # prints an rclone command to copy current+prior month from Polygon S3
python load.py           # ETL: CSV.gz → Parquet, then fetches missing options contracts
python validate.py       # checks/repairs contract parquet schema consistency
python read.py           # prints row counts for the four market data tables
```

Install dependencies: `pip install -r requirements.txt`.

## Architecture

### Data Flow

```
Polygon S3 flat files
  → rclone (download.py prints the command, user runs it)
  → CSV.gz in {data_root}/downloads/{us_stocks_sip,us_options_opra}/{day_aggs_v1,minute_aggs_v1}/YYYY/MM/
  → load.py → db/db.py
  → Parquet in {data_root}/parquet/{stocks_day,stocks_min,options_day,options_min}/year=YYYY/YYYY-MM-DD.parquet
  → contracts fetched from Polygon REST API → {data_root}/parquet/contracts/underlying_ticker=XXX/YYYY.parquet
  → DuckDB in-memory views over all Parquet via hive partitioning
```

### Key Modules

- **db/db.py** — Core ETL. `create_or_get()` initializes DuckDB + directory structure and runs pending schema migrations. `load_latest()` ingests CSV.gz into sorted Parquet, then calls `load_missing_contracts()`. All parquet writes are atomic (write to `.tmp`, then `os.rename`).
- **db/migrations.py** — Flyway-style schema migration system. `SCHEMA_VERSION` and `MIGRATIONS` list define the target version and ordered migration functions. `run_pending_migrations()` is called by `create_or_get()` — it reads `.schema_version` from the parquet root, applies any pending migrations, and updates the version file. To add a migration: bump `SCHEMA_VERSION`, add a `(version, description, function)` tuple to `MIGRATIONS`.
- **db/connection.py** — `DuckDBConnectionWrapper`: wraps DuckDB connection, adds `init_views()` (creates views over hive-partitioned Parquet), `genex()` (generator-based row-at-a-time query), and context manager support. Delegates unknown attributes to the underlying connection via `__getattr__`.
- **db/contracts.py** — Options contract fetching from Polygon REST API. Chooses between individual and bulk fetch based on `_MIN_TO_USE_BULK_DOWNLOAD` threshold, auto-tuned by `ContractFetchTimings`. `parse_ticker()` extracts contract info from ticker strings (e.g., `O:AAPL230915C00150000`) as fallback when API returns nothing.
- **utils/parquet.py** — `update_parquet_file()`: upsert/create parquet with optional sort and dedup. `rewrite_parquet_file()`: schema migration with safe casting. `validate_parquet_files()`: schema consistency checks across a directory tree.
- **utils/polygon_files.py** — `get_files_in_date_order()`: walks CSV.gz directories chronologically. `extract_date_from_filename()`: parses YYYY-MM-DD from filenames.
- **utils/views_gen.py** — Standalone utility. Generates a `views.sql` file (git-ignored) containing DuckDB `CREATE VIEW` statements for all five Parquet-backed tables.

### Parquet Tables

| Table | Partitioning | Source |
|-------|-------------|--------|
| `stocks_day` | `year=YYYY/YYYY-MM-DD.parquet` | SIP day aggs |
| `stocks_min` | `year=YYYY/YYYY-MM-DD.parquet` | SIP minute aggs |
| `options_day` | `year=YYYY/YYYY-MM-DD.parquet` | OPRA day aggs |
| `options_min` | `year=YYYY/YYYY-MM-DD.parquet` | OPRA minute aggs |
| `contracts` | `underlying_ticker=XXX/YYYY.parquet` | Polygon REST API |

Stock/options schema: `ticker, volume (float64), open, close, high, low, window_start (ms), transactions, timestamp (ms, America/New_York naive)`.

Contracts schema: `ticker, cfi, contract_type, exercise_style, expiration_date, primary_exchange, shares_per_contract, strike_price`.

## Conventions

- Config is a plain Python module (`config.py`), imported directly — not a class or dict.
- Parquet writes always use atomic temp-file-then-rename pattern.
- `window_start` is stored in milliseconds (converted from nanoseconds during CSV ingestion). `timestamp` is America/New_York but stored as a naive (timezone-unaware) timestamp.
