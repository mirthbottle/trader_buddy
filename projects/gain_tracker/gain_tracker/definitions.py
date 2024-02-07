"""
gain tracker ETL executable

execute from the repo root dir /trader_buddy/
> dagster dev -f projects/gain_tracker/gain_tracker/gain_tracker.py 
"""
import os
import pandas as pd

from dagster_gcp_pandas import BigQueryPandasIOManager
from dagster import Definitions, SourceAsset, asset

import yfinance as yf

from gain_tracker.etrade_api import ETradeAPI

# from assets.positions import etrade_positions

DEFAULT_BENCHMARK_TICKER="IVV"

# name is wrong
positions_data = SourceAsset(key="positions_dec")

@asset
def etrade_positions():
    """Pull accounts and positions in etrade

    see if dagster can trigger opening a website and have a user input
    """
    env = os.getenv("ENV", "dev")
    session_token = os.getenv("SESSION_TOKEN")
    session_token_secret = os.getenv("SESSION_TOKEN_SECRET")
    etrader = ETradeAPI(
        env, session_token=session_token, session_token_secret=session_token_secret)
    session = etrader.create_authenticated_session()
    accounts = etrader.list_accounts()
    return pd.DataFrame(accounts)


@asset
def benchmark_history(positions_dec:pd.DataFrame):
    """Pull benchmark prices, dummy asset
    """
    earliest_date = positions_dec["position_entry_date"].min()
    bm = yf.Ticker(DEFAULT_BENCHMARK_TICKER)
    bm_hist = bm.history(start=earliest_date)
    return bm_hist.reset_index()

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
        etrade_positions, positions_data, 
        positions_count, benchmark_history],
    resources={
        "io_manager": BigQueryPandasIOManager(
            project=os.environ["GCP_PROJECT"],  # required
            # location="us-west1",  # optional, defaults to the default location for the project - see https://cloud.google.com/bigquery/docs/locations for a list of locations
            dataset="gain_tracker_dev",  # optional, defaults to PUBLIC
            timeout=15.0,  # optional, defaults to None
        )
    },
)