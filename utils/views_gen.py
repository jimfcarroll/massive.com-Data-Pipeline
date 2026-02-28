import os

import config

views_file = 'views.sql'

view_sql = """
CREATE VIEW stocks_day AS 
SELECT * 
FROM read_parquet('{os.path.join(config.data_root, config.parquet_subdir, 'stocks_day','**','*')}', hive_partitioning=True);

CREATE VIEW stocks_min AS 
SELECT * 
FROM read_parquet('{os.path.join(config.data_root, config.parquet_subdir, 'stocks_min','**','*')}', hive_partitioning=True);

CREATE VIEW options_day AS 
SELECT * 
FROM read_parquet('{os.path.join(config.data_root, config.parquet_subdir, 'options_day','**','*')}', hive_partitioning=True);

CREATE VIEW options_min AS 
SELECT * 
FROM read_parquet('{os.path.join(config.data_root, config.parquet_subdir, 'options_min','**','*')}', hive_partitioning=True);

CREATE VIEW contracts AS 
SELECT * 
FROM read_parquet('{os.path.join(config.data_root, config.parquet_subdir, 'contracts','**','*')}', hive_partitioning=True);
"""
"""
SQL template for creating database views.

Creates the following views:
- stocks_day: Daily stock data
- stocks_min: Minute-by-minute stock data
- options_day: Daily options data
- options_min: Minute-by-minute options data
- contracts: Options contracts data

Each view reads from corresponding parquet files with hive partitioning enabled.
"""

if os.path.exists(views_file):
    os.remove(views_file)

with open(views_file, 'w') as file:
    file.write(view_sql)
