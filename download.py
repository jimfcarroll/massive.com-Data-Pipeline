"""
Script for downloading financial data using rclone.

Constructs an rclone command to copy data from S3 (massive.com) to local storage.
Filters data based on:
- Data types: stocks (SIP) and options (OPRA)
- Aggregation: daily and minute
- Time period: current and previous month/year
"""

import datetime
import os

import config

cur = datetime.datetime.now()

cur_month = cur.month
cur_year = cur.year
last_month = cur_month - 1
if last_month == 0:
    last_month = 12

print(f'rclone copy s3massive:flatfiles {os.path.join(config.data_root, config.downloads_subdir)} --include "{{us_stocks_sip,us_options_opra}}/{{day_aggs_v1,minute_aggs_v1}}/{{{cur_year - 1},{cur_year}}}/{{{last_month:02d},{cur_month:02d}}}/*"')

