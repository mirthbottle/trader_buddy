"""

"""

import pandas as pd
from dagster import asset

import yfinance as yf

from ..etrade_api import ETradeAPI

DEFAULT_BENCHMARK_TICKER="IVV"


@asset
def etrade_positions():
    """Pull accounts and positions in etrade

    see if dagster can trigger opening a website and have a user input
    """
    etrader = ETradeAPI("dev")
    session = etrader.get_session()
    accounts = etrader.list_accounts()
    return pd.DataFrame(accounts)

@asset
def benchmarks(positions:pd.DataFrame):
    """Pull benchmark gains
    """
    earliest_date = positions["position_entry_date"].min()
    bm = yf.Ticker(DEFAULT_BENCHMARK_TICKER)
    bm_hist = bm.history(start=earliest_date)
    # return bm_hist.reset_index()

@asset
def market_values(positions: pd.DataFrame):
    """Compute position_gains

    """
    pass


@asset
def sell_recommendations(market_values: pd.DataFrame):
    """
    """
    pass