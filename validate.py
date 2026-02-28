import os

import config
from db.db import create_or_get
from db.contracts import update_parquet_file, _contracts_schema
from utils.parquet import rewrite_parquet_file

if __name__ == "__main__":
    """
    Main validation script for contract data.
    
    Performs the following operations:
    1. Establishes database connection
    2. Initializes views
    3. Iterates through contracts by underlying ticker and year
    4. Validates and updates parquet files
    5. Handles schema mismatches by rewriting files when necessary
    
    Error handling includes:
    - Validation of parquet file structure
    - Schema consistency checks
    - Automatic cleanup of invalid files
    """
    root = config.data_root
    con = None

    try:
        con = create_or_get(root, config.parquet_subdir, fail_if_not_exists=True)
        con.init_views()
        for cur in con.genex("""
                            SELECT DISTINCT 
                                underlying_ticker,
                                YEAR(expiration_date) AS year
                            FROM contracts ORDER BY underlying_ticker, year;
                             """):
            ut = cur[0]
            year = cur[1]
            pf_path = os.path.join(con.data_dirs['contracts'], f"underlying_ticker={ut}", f"{year}.parquet")
            try:
                update_parquet_file(pf_path, [], 'expiration_date', _contracts_schema, 'ticker')
            except ValueError as e:
                print(f'Failed for {pf_path}')
                rewrite_parquet_file(pf_path, _contracts_schema)
                update_parquet_file(pf_path, [], 'expiration_date', _contracts_schema, 'ticker')
            
    finally:
        con.close()
