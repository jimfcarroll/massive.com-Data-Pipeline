import os
from datetime import datetime
from typing import Iterator, Union

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pv
import pyarrow.parquet as pq
import pytz

from db.connection import DuckDBConnectionWrapper
from db.contracts import (_contracts_schema, fetch_all_options_contracts,
                          load_missing_contracts, options_contract_to_dict,
                          to_contract_parquet_file_path, update_parquet_file)
from db.migrations import run_pending_migrations
from utils.parquet import parquet_file_exists
from utils.polygon_files import (extract_date_from_filename,
                                 get_files_in_date_order)

# Define schema for CSV to Parquet conversion
PARQUET_SCHEMA = pa.schema([
    ("ticker", pa.string()),
    ("volume", pa.float64()),
    ("open", pa.float64()),
    ("close", pa.float64()),
    ("high", pa.float64()),
    ("low", pa.float64()),
    ("window_start", pa.int64()),  # Still in milliseconds
    ("transactions", pa.int64()),
    ("timestamp", pa.timestamp('ms')),  # For the converted timestamp
])

def create_or_get(root: str, parquet_subdir: str, fail_if_not_exists: bool = False) -> DuckDBConnectionWrapper:
    """
    Creates or retrieves a DuckDB connection with initialized data directories.
    
    Args:
        root (str): Root directory path for the database
        parquet_subdir (str): Subdirectory for parquet files
        fail_if_not_exists (bool, optional): If True, raises error when directories don't exist
        
    Returns:
        DuckDBConnectionWrapper: Initialized database connection wrapper
        
    Raises:
        FileNotFoundError: If fail_if_not_exists is True and directories don't exist
    """
    if root is None:
        raise ValueError("Must supply a non-None root directory")
    if not isinstance(root, str):
        raise TypeError("The root directory name must be a string.")

    full_path = os.path.join(root, parquet_subdir)

    if fail_if_not_exists and not os.path.exists(full_path):
        raise FileNotFoundError(f"The database directory '{full_path}' does not exist.")

    os.makedirs(full_path, exist_ok=True)

    # Initialize subdirectories for each data type
    data_dirs = {
        'stocks_day': os.path.join(full_path, 'stocks_day'),
        'stocks_min': os.path.join(full_path, 'stocks_min'),
        'options_day': os.path.join(full_path, 'options_day'),
        'options_min': os.path.join(full_path, 'options_min'),
        'contracts': os.path.join(full_path, 'contracts'),
    }

    for name, path in data_dirs.items():
        os.makedirs(path, exist_ok=True)

    run_pending_migrations(data_dirs, full_path)

    if not any(os.scandir(data_dirs['contracts'])):
        # if the directory is empty then we need to prime it
        contract = next(fetch_all_options_contracts(pagination_limit = 1))
        contract_dict = options_contract_to_dict(contract, skip_underlying_ticker=True)
        pf_path = to_contract_parquet_file_path(contract,data_dirs['contracts'])
        update_parquet_file(pf_path, contract_dict, 'expiration_date', _contracts_schema)

    # Create a DuckDB connection
    connection = duckdb.connect(database=':memory:')

    return DuckDBConnectionWrapper(connection, data_dirs, full_path)

def load_latest(con: DuckDBConnectionWrapper, downloads_dir: str, file_count: float = float('inf')) -> int:
    """
    Loads the most recent data files into the database.
    
    Args:
        con (DuckDBConnectionWrapper): Database connection wrapper
        downloads_dir (str): Directory containing downloaded files
        file_count (float, optional): Maximum number of files to load. Defaults to infinity.
        
    Returns:
        int: Number of files successfully loaded
        
    Note:
        Processes files in chronological order and stops when file_count is reached
        or all files are processed.
    """
    file_count -= _load_latest_from(con, os.path.join(downloads_dir, 'us_stocks_sip', 'day_aggs_v1'), 'stocks_day', file_count)
    file_count -= _load_latest_from(con, os.path.join(downloads_dir, 'us_stocks_sip', 'minute_aggs_v1'), 'stocks_min', file_count)
    file_count -= _load_latest_from(con, os.path.join(downloads_dir, 'us_options_opra', 'day_aggs_v1'), 'options_day', file_count)
    file_count -= _load_latest_from(con, os.path.join(downloads_dir, 'us_options_opra', 'minute_aggs_v1'), 'options_min', file_count)
    load_missing_contracts(con, 'options_day')
    load_missing_contracts(con, 'options_min')
    return file_count

def _save_stock_data_to_parquet(batch: Union[pa.RecordBatch, pa.Table], 
                               output_dir: str, 
                               year: int, 
                               file_date: datetime.date) -> None:
    """Saves a PyArrow RecordBatch or Table to a Hive-compliant Parquet structure atomically using a temporary file."""
    year_dir = os.path.join(output_dir, f"year={year}")

    # Use the extracted file_date for the filename
    filename = f"{file_date.strftime('%Y-%m-%d')}.parquet"
    file_path = os.path.join(year_dir, filename)
    _save_to_parquet(batch, file_path)

def _save_to_parquet(batch: Union[pa.RecordBatch, pa.Table], 
                    file_path: str) -> None:
    """
    Atomically saves a PyArrow batch or table to a parquet file using a temporary file.
    
    Args:
        batch: PyArrow RecordBatch or Table to save
        file_path: Destination path for the parquet file
    
    Raises:
        TypeError: If batch is neither a RecordBatch nor a Table
        Exception: If any I/O or write operations fail
        
    Note:
        Uses a temporary file with .tmp extension to ensure atomic writes.
        Cleans up temporary files if the operation fails.
    """
    # Ensure the parent directory exists
    parent_dir = os.path.dirname(file_path)
    os.makedirs(parent_dir, exist_ok=True)

    # Temporary file path
    temp_file_path = f"{file_path}.tmp"

    # Ensure the input is a Table
    if isinstance(batch, pa.RecordBatch):
        table = pa.Table.from_batches([batch])
    elif isinstance(batch, pa.Table):
        table = batch
    else:
        raise TypeError(f"Unsupported batch type: {type(batch)}")

    try:
        # Remove any existing temporary file
        if os.path.exists(temp_file_path):
            os.remove(temp_file_path)
            print(f"Removed existing temporary file: {temp_file_path}")

        # Write data to the temporary Parquet file
        pq.write_table(table, temp_file_path)
        print(f"Data written to temporary file: {temp_file_path}")

        # Move the temporary file to the final destination atomically
        os.rename(temp_file_path, file_path)
        print(f"Temporary file moved to: {file_path}")
    except Exception as e:
        # Clean up the temporary file if something goes wrong
        if os.path.exists(temp_file_path):
            os.remove(temp_file_path)
            print(f"Cleaned up temporary file: {temp_file_path}")
        print(f"Error while saving Parquet file: {e}")
        raise

def _process_chunks(stream: pv.CSVStreamingReader, 
                   ny_tz: pytz.timezone) -> Iterator[pa.RecordBatch]:
    """
    Processes CSV chunks to standardize data format and add timestamps.
    
    Args:
        stream: PyArrow CSV stream to process
        ny_tz: pytz timezone object for New York time
        
    Yields:
        PyArrow RecordBatch: Processed chunk with:
            - window_start converted from nanoseconds to milliseconds
            - Added timestamp column in America/New_York timezone (stored as naive timestamp)
            - Schema matching PARQUET_SCHEMA
            
    Note:
        Timestamps are stored without timezone information after conversion
        from America/New_York time to maintain compatibility with various
        data processing tools.
    """
    for chunk in stream:
        # Convert window_start from nanoseconds to milliseconds
        window_start_ms = pc.divide(chunk.column("window_start"), 1_000_000).cast(pa.int64())
        chunk = chunk.set_column(chunk.schema.get_field_index("window_start"), "window_start", window_start_ms)

        # Add a timestamp column in America/New_York timezone
        timestamps = pa.array([
            datetime.fromtimestamp(ws.as_py() / 1_000, tz=ny_tz).replace(tzinfo=None)
            for ws in window_start_ms
        ], pa.timestamp("ms"))  # Naive timestamp without timezone

        chunk = chunk.append_column("timestamp", timestamps)

        # Ensure schema compliance
        yield chunk.cast(PARQUET_SCHEMA)

def _read_file(con: DuckDBConnectionWrapper, 
               file_path: str, 
               data_type: str) -> None:
    """Reads a CSV file, processes chunks, sorts them globally, and saves to a single Parquet file."""
    read_options = pv.ReadOptions(block_size=2 * 1024 * 1024)  # 2 MB blocks
    convert_options = pv.ConvertOptions(column_types={
        "ticker": pa.string(),
        "volume": pa.float64(),
        "open": pa.float64(),
        "close": pa.float64(),
        "high": pa.float64(),
        "low": pa.float64(),
        "window_start": pa.int64(),
        "transactions": pa.int64(),
    })

    # Read CSV file in chunks
    stream = pv.open_csv(file_path, read_options=read_options, convert_options=convert_options)

    # Extract year and file date
    file_date = extract_date_from_filename(file_path)
    year = file_date.year

    # Define timezone
    ny_tz = pytz.timezone("America/New_York")

    # Process chunks using a generator
    processed_chunks = _process_chunks(stream, ny_tz)

    # Combine chunks into a single Table
    combined_table = pa.Table.from_batches(processed_chunks)

    # Sort the combined data by ticker and window_start
    sort_indices = pc.sort_indices(combined_table, sort_keys=[
        ("ticker", "ascending"),
        ("window_start", "ascending")
    ])
    sorted_table = pc.take(combined_table, sort_indices)

    # Save the sorted table to a single Parquet file
    output_dir = con.data_dirs[data_type]
    _save_stock_data_to_parquet(sorted_table, output_dir, year, file_date)

def _load_latest_from(con: DuckDBConnectionWrapper, 
                     root_dir: str, 
                     data_type: str, 
                     file_count: float = float('inf')) -> int:
    """Loads the latest files from CSV to Parquet and returns the number of processed files."""
    processed_count = 0
    
    if file_count > 0:
        # latest = get_latest_file_date(con, data_type)
        
        for file_path in get_files_in_date_order(root_dir):
            file_date = extract_date_from_filename(file_path)
            # if latest is None or file_date > latest:
            if not parquet_file_exists(con, data_type, file_date):
                print(f"Processing file: {file_path}")
                _read_file(con, file_path, data_type)
                processed_count += 1
                if processed_count >= file_count:
                    break

    print(f"Processed {processed_count} files.")
    return processed_count

if __name__ == "__main__":
    import config

    con = create_or_get(config.data_root, config.parquet_subdir, fail_if_not_exists=False)
    load_latest(con, os.path.join(config.data_root, config.downloads_subdir), file_count=50)
