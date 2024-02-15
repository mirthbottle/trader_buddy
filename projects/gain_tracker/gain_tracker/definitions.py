"""
gain tracker ETL executable

execute from the repo root dir /trader_buddy/
> dagster dev -f projects/gain_tracker/gain_tracker/gain_tracker.py 
"""
import os
import pandas as pd

from dagster_gcp_pandas import BigQueryPandasIOManager
from dagster import Definitions, asset

from resources.etrade_resource import ETrader

from assets.positions import (
    positions, etrade_accounts, etrade_positions,
    benchmark_values)


@asset
def positions_count(
    positions_dec:pd.DataFrame
):
    """Dummy asset
    """
    print(len(positions_dec))
    return positions_dec

defs = Definitions(
    assets=[
        etrade_accounts, etrade_positions, 
        positions_count, benchmark_values],
    resources={
        "io_manager": BigQueryPandasIOManager(
            project=os.environ["GCP_PROJECT"],  # required
            # location="us-west1",  # optional, defaults to the default location for the project - see https://cloud.google.com/bigquery/docs/locations for a list of locations
            dataset="gain_tracker_dev",  # optional, defaults to PUBLIC
            timeout=15.0,  # optional, defaults to None
        ),
        "etrader": ETrader()
    },
)