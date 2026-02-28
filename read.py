from duckdb import DuckDBPyConnection

import config
from db.db import create_or_get


def count_rows(con: DuckDBPyConnection):
    # List of table names
    tables = ['stocks_day', 'stocks_min', 'options_day', 'options_min']

    # Query to get the total row count for all tables
    total_rows = 0
    for table in tables:
        # Get the row count for each table
        row_count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"Table {table}: {row_count:,} rows")  # Human-readable format with commas
        total_rows += row_count

    return total_rows


if __name__ == "__main__":
    root = config.data_root

    con = create_or_get(root, config.parquet_subdir, fail_if_not_exists=False)
    con.init_views()

    total_rows = count_rows(con)

    # Display the total row count across all tables
    print(f"\nTotal rows across all tables: {total_rows:,} rows")

#     try:
# #        cursor = con.execute("SELECT * FROM 'stocks_day' WHERE ticker = 'AAPL' ORDER BY window_start")
#         cursor = con.execute("SELECT count(*) FROM 'stocks_min'")
#         while True:
#             rows = cursor.fetchmany(5)  # Fetch a chunk of rows
#             if not rows:  # Stop when no more rows are returned
#                 break
#             for row in rows:
#                 print(row)

#     finally:
#         con.close()
