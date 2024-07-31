"""
gain tracker ETL executable

execute from the repo root dir /trader_buddy/
> dagster dev -f projects/gain_tracker/gain_tracker/gain_tracker.py 
"""
import os
import pandas as pd

from dagster_gcp_pandas import BigQueryPandasIOManager
from dagster import Definitions, asset, FilesystemIOManager

from gain_tracker.resources.etrade_resource import ETrader
from gain_tracker.resources.gsheets_resource import GSheetsResource

from gain_tracker.assets.positions import (
    etrade_accounts, etrade_positions, etrade_transactions,
    sold_transactions,
    positions_scd4, open_positions, gains, sell_recommendations,
    buy_recommendations_previously_sold, all_recommendations,
    benchmark_values)
from gain_tracker.jobs.daily_jobs import (
    pull_etrade_job, recommendations_job
)

from gain_tracker.assets.economic_indicators import (
    inflation_data, inflation_gsheet
)

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
        # positions_count, 
        etrade_accounts, etrade_positions, etrade_transactions,
        sold_transactions,
        positions_scd4, open_positions,
        gains, sell_recommendations, buy_recommendations_previously_sold,
        all_recommendations,
        benchmark_values,
        inflation_data, inflation_gsheet
        ],
    jobs=[pull_etrade_job, recommendations_job],
    resources={
        "io_manager": BigQueryPandasIOManager(
            project=os.environ.get("GCP_PROJECT", "main-street-labs-test"),  # required
            # location="us-west1",  # optional, defaults to the default location for the project - see https://cloud.google.com/bigquery/docs/locations for a list of locations
            dataset="gain_tracker_dev",  # optional, defaults to PUBLIC
            timeout=15.0,  # optional, defaults to None
        ),
        "fs_io_manager": FilesystemIOManager(),
        "etrader": ETrader.configure_at_launch(),
        "gsheets": GSheetsResource(
            google_service_file_loc=os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "service_file_key.json"))
    },
)