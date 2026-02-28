import os

from utils.parquet import correct_parquet_files

# Bump this when adding new migrations.
SCHEMA_VERSION = 1

# Tables that use PARQUET_SCHEMA from db.py (excludes contracts, which has its own schema).
_MARKET_DATA_TABLES = ['stocks_day', 'stocks_min', 'options_day', 'options_min']


def _migrate_v1(data_dirs):
    """Volume int64 → float64 (Polygon fractional shares, Feb 2026)."""
    from db.db import PARQUET_SCHEMA
    for table in _MARKET_DATA_TABLES:
        path = data_dirs[table]
        print(f"  Migrating {table} ...")
        correct_parquet_files(path, PARQUET_SCHEMA)


# Ordered list of (version, description, function).
# Each function receives the data_dirs dict.
MIGRATIONS = [
    (1, "volume int64 to float64 (Polygon fractional shares)", _migrate_v1),
]


def _is_empty_database(data_dirs):
    """Returns True if none of the market data directories contain parquet files."""
    for table in _MARKET_DATA_TABLES:
        path = data_dirs.get(table)
        if path and os.path.isdir(path):
            for _, _, files in os.walk(path):
                if any(f.endswith('.parquet') for f in files):
                    return False
    return True


def run_pending_migrations(data_dirs, parquet_root):
    """Check on-disk schema version and apply any pending migrations."""
    version_file = os.path.join(parquet_root, '.schema_version')

    current_version = 0
    if os.path.exists(version_file):
        with open(version_file) as f:
            current_version = int(f.read().strip())

    if current_version >= SCHEMA_VERSION:
        return

    # Fresh install — stamp at current version, nothing to migrate.
    if _is_empty_database(data_dirs):
        _write_version(version_file, SCHEMA_VERSION)
        return

    for version, description, migrate_fn in MIGRATIONS:
        if version > current_version:
            print(f"Running migration {version}: {description}")
            migrate_fn(data_dirs)
            _write_version(version_file, version)

    print(f"Schema migrated to version {SCHEMA_VERSION}")


def _write_version(version_file, version):
    os.makedirs(os.path.dirname(version_file), exist_ok=True)
    with open(version_file, 'w') as f:
        f.write(str(version))
