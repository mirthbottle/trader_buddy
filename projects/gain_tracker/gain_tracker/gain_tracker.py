"""
gain tracker ETL executable

execute from the repo root dir /trader_buddy/
> dagster dev -f projects/gain_tracker/gain_tracker/gain_tracker.py 
"""
import os
import pandas as pd

from dagster_gcp_pandas import BigQueryPandasIOManager
from dagster import Definitions, SourceAsset, asset

positions_data = SourceAsset(key="positions_dev")

@asset
def positions_count(positions_dev:pd.DataFrame):
    print(len(positions_dev))
    return positions_dev

defs = Definitions(
    assets=[positions_data, positions_count],
    resources={
        "io_manager": BigQueryPandasIOManager(
            project=os.environ["GCP_PROJECT"],  # required
            # location="us-west1",  # optional, defaults to the default location for the project - see https://cloud.google.com/bigquery/docs/locations for a list of locations
            dataset="trader_buddy",  # optional, defaults to PUBLIC
            timeout=15.0,  # optional, defaults to None
        )
    },
)