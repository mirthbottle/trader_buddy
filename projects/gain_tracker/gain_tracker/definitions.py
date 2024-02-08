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

from resources.etrade_resource import ETrader

# from assets.positions import etrade_positions

DEFAULT_BENCHMARK_TICKER="IVV"

# name is wrong
positions_data = SourceAsset(key="positions_dec")

@asset
def etrade_accounts(etrader: ETrader):
    """Pull accounts in etrade

    see if dagster can trigger opening a website and have a user input
    """
    accounts = etrader.list_accounts()
    return pd.DataFrame(accounts)

@asset
def etrade_positions(etrader: ETrader, etrade_accounts: pd.DataFrame):
    """Pull positions in etrade for each account
    """
    keys = etrade_accounts["accountidkey"].values

    all_positions = []
    for k in keys:
        portfolio = etrader.view_portfolio(k)
        if portfolio is not None:
            ps = pd.DataFrame(portfolio)
            ps.loc[:, "account_id_key"] = k
            all_positions.append(ps)

    positions = pd.concat(all_positions)
    return positions

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
        etrade_accounts, etrade_positions, positions_data, 
        positions_count, benchmark_history],
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