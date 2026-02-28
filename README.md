# massive.com-Data-Pipeline

Data pipeline for ingesting Polygon.io US stocks and options market data into hive-partitioned Parquet files, queryable through DuckDB.

## What It Does

Downloads flat files from Polygon's S3 bucket via rclone, converts CSV.gz aggregates (daily and minute-level) into sorted, hive-partitioned Parquet, fetches missing options contract metadata from the Polygon REST API, and exposes everything as DuckDB in-memory views.

### Data Coverage

- **Stocks**: Daily and minute aggregates (SIP)
- **Options**: Daily and minute aggregates (OPRA)
- **Contracts**: Options contract metadata (Polygon REST API)

## Setup

### Prerequisites

- Python 3.x
- [rclone](https://rclone.org/) configured with a remote named `s3polygon` pointing at the Polygon.io S3 flat file bucket (see [Polygon flat files documentation](https://polygon.io/flat-files))
- A [Polygon.io](https://polygon.io/) API key (for contract metadata)

### Installation

```bash
pip install -r requirements.txt
```

### Configuration

Copy `config_sample.py` to `config.py` and fill in your values:

```python
data_root = '/path/to/your/data'
downloads_subdir = 'downloads'
parquet_subdir = 'parquet'

PAPI_KEY = "your-polygon-api-key"
```

`config.py` is git-ignored. Each developer maintains their own copy.

A `.env` file with `PYTHONPATH=.` is also needed for IDE/shell support (also git-ignored).

## Usage

```bash
# 1. Print the rclone command for downloading current + prior month data
python download.py

# 2. Run the printed rclone command to download from Polygon S3

# 3. ETL: convert CSV.gz to Parquet + fetch missing options contracts
python load.py

# 4. Validate/repair contract parquet schema consistency
python validate.py

# 5. Print row counts for all market data tables
python read.py
```

## Data Layout

```
{data_root}/
├── downloads/                # Raw CSV.gz from Polygon S3
│   ├── us_stocks_sip/
│   │   ├── day_aggs_v1/YYYY/MM/*.csv.gz
│   │   └── minute_aggs_v1/YYYY/MM/*.csv.gz
│   └── us_options_opra/
│       ├── day_aggs_v1/YYYY/MM/*.csv.gz
│       └── minute_aggs_v1/YYYY/MM/*.csv.gz
└── parquet/                  # Processed hive-partitioned Parquet
    ├── stocks_day/year=YYYY/YYYY-MM-DD.parquet
    ├── stocks_min/year=YYYY/YYYY-MM-DD.parquet
    ├── options_day/year=YYYY/YYYY-MM-DD.parquet
    ├── options_min/year=YYYY/YYYY-MM-DD.parquet
    └── contracts/underlying_ticker=XXX/YYYY.parquet
```

## Tech Stack

- **Python** — pipeline orchestration and data processing
- **DuckDB** — in-memory analytical queries over Parquet
- **PyArrow** — Parquet read/write and schema management
- **Polygon.io** — market data source (S3 flat files + REST API)
- **rclone** — S3 file transfer
