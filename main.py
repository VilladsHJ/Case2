import datetime as dt
import requests
import pandas as pd
import json
import parquet
import pyarrow as pa
import pyarrow.parquet as pq
import ETL





if __name__ == '__main__':
    dfs, non_dfs = ETL.ETL_script()
