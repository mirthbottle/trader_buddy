"""

"""

import pandas as pd
from dagster import SourceAsset, asset

import yfinance as yf

from resources.etrade_resource import ETrader
import position_gain as pg

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
    """Pull benchmark gains
    """
    earliest_date = positions_dec["position_entry_date"].min()
    bm = yf.Ticker(DEFAULT_BENCHMARK_TICKER)
    bm_hist = bm.history(start=earliest_date)
    return bm_hist.reset_index()

@asset
def market_values(etrade_positions: pd.DataFrame):
    """Compute position_gains

    """
    pass


@asset
def sell_recommendations(market_values: pd.DataFrame):
    """
    """
    pass