import datetime
import os
import re
from datetime import date
from typing import Generator

from db.db import DuckDBConnectionWrapper


def get_files_in_date_order(root_dir: str) -> Generator[str, None, None]:
    """
    Yields file paths from the directory structure sorted by date.
    
    Traverses the directory structure and returns file paths in chronological order,
    assuming a YYYY/MM/YYYY-MM-dd.csv.gz file naming convention.
    
    Args:
        root_dir (str): Root directory to start traversal from
        
    Yields:
        str: Full file paths in date order
    """
    for root, _, files in sorted(os.walk(root_dir)):
        for file in sorted(files):
            if file.endswith('.csv.gz'):
                yield os.path.join(root, file)

def extract_date_from_filename(filename: str) -> date:
    """
    Extracts date from a filename containing a YYYY-MM-DD format.
    
    Args:
        filename (str): Filename or path containing a date
        
    Returns:
        date: Extracted date as a datetime.date object
        
    Raises:
        ValueError: If filename doesn't contain a valid date format
    """
    # Regular expression to match 'YYYY-MM-DD'
    match = re.search(r'(\d{4})-(\d{2})-(\d{2})', filename)
    if not match:
        raise ValueError(f"Filename does not contain a valid date: {filename}")
    
    # Extract year, month, day and convert to a date object
    year, month, day = map(int, match.groups())
    return date(year, month, day)

def get_latest_file_date(con: DuckDBConnectionWrapper, data_type: str) -> datetime.date:
    """
    Determines the latest file date based on the file names in the specified data_type directory.

    Args:
        con: DuckDBConnectionWrapper instance containing directory metadata.
        data_type: The data type (e.g., 'stocks_day', 'stocks_min') to check.

    Returns:
        The latest file date as a datetime.date object or None if no files are found.
    """
    data_dir = con.data_dirs.get(data_type)
    if not data_dir or not os.path.exists(data_dir):
        raise ValueError(f"Data directory for {data_type} does not exist: {data_dir}")

    latest_date = None

    for root, _, files in os.walk(data_dir):
        for file in files:
            if file.endswith('.parquet'):
                try:
                    # Extract the date from the file name
                    file_date = extract_date_from_filename(file)
                    if latest_date is None or file_date > latest_date:
                        latest_date = file_date
                except ValueError:
                    # Skip files with invalid or missing dates
                    continue

    return latest_date

