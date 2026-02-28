import os

import config
from db.db import create_or_get, load_latest

if __name__ == "__main__":
    """
    Main script for loading data into the database.
    
    Creates or retrieves a database connection and loads the latest data.
    Handles connection cleanup through context management.
    """
    root = config.data_root
    con = None

    try:
        con = create_or_get(root, config.parquet_subdir, fail_if_not_exists=False)
        file_count = float('inf')
        if file_count == load_latest(con, os.path.join(root, config.downloads_subdir), file_count):
            print("Done loading")
        print("-------------------------------------------------------------", flush=True)

    finally:
        con.close()
        