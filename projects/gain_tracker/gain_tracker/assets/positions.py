"""

"""

from typing import Optional
import re
import pandas as pd
from dagster import (
    asset, multi_asset, AssetOut, AssetKey,
    DailyPartitionsDefinition, AssetExecutionContext
    )

import yfinance as yf

from ..resources.etrade_resource import ETrader
from .. import position_gain as pg

DEFAULT_BENCHMARK_TICKER="IVV"

# name is wrong
# positions_data = SourceAsset(key="positions_dec")

def camel_to_snake(camel_case):
    """convert to something bigquery-friendly

    move this to a formatting io location, maybe in resources?
    ideally it would go in a new lib project. not sure how to import that
    """
    # Use regular expressions to split the string at capital letters
    cc_adj = camel_case[0].upper()+camel_case[1:]
    words = re.findall(r'[A-Z][a-z0-9]*', cc_adj)
    # Join the words with underscores and convert to lowercase
    snake_case = '_'.join(words).upper()
    return snake_case


@asset
def etrade_accounts(etrader: ETrader):
    """Pull accounts in etrade

    see if dagster can trigger opening a website and have a user input
    """
    accounts = pd.DataFrame(etrader.list_accounts())

    snake_cols = [camel_to_snake(c) for c in accounts.columns]
    accounts.rename(columns=snake_cols, inplace=True)
    return accounts.set_index("ACCOUNT_ID_KEY")

@asset
def etrade_positions(etrader: ETrader, etrade_accounts: pd.DataFrame):
    """Pull positions in etrade for each account
    """
    keys = etrade_accounts["ACCOUNT_ID_KEY"].values

    all_positions = []
    for k in keys:
        portfolio = etrader.view_portfolio(k)
        if portfolio is not None:
            ps = pd.DataFrame(portfolio)
            # ps.loc[:, "account_id_key"] = k
            all_positions.append(ps)

    positions = pd.concat(all_positions)
    
    return positions


@multi_asset(
        outs={
            "open_positions": AssetOut(),
            "closed_positions": AssetOut()
        },
        can_subset=True
)
def updated_positions(
    etrade_positions: pd.DataFrame, 
    etrade_transactions: Optional[pd.DataFrame]):
    """
    needs a bigquery resource
    some positions may have closed and are not in etrade_positions anymore

    positions gets updated

    use positionid
    not sure what happens to the positionid when only some shares are sold
    """
    from ..definitions import defs
    old_open_positions = defs.load_asset_value(AssetKey("open_positions")).set_index('positionid')
    old_closed_positions = defs.load_asset_value(AssetKey("closed_positions")).set_index('positionid')

    # select the positions that aren't in etrade_positions?
    new_closed_positions = old_open_positions.loc[
        ~old_open_positions.index.isin(etrade_positions.index)]#.copy()
    closed_positions = pd.concat([old_closed_positions, new_closed_positions])

    # in etrade_positions but not in positions
    new_open_positions = etrade_positions.loc[
        ~etrade_positions.index.isin(old_open_positions.index)
    ]
    open_positions = pd.concat([old_open_positions, new_open_positions])

    return open_positions, closed_positions

@asset(partitions_def=DailyPartitionsDefinition(start_date="2023-10-01"))
def market_values(context: AssetExecutionContext, open_positions: pd.DataFrame):
    """Market values and gains
    
    save this asset daily? ok
    closed positions may not have the last entry of market_values
    is that ok?
    """
    partition_date_str = context.partition_key

    return open_positions

@asset(partitions_def=DailyPartitionsDefinition(start_date="2023-10-01"))
def benchmark_values(context: AssetExecutionContext, open_positions:pd.DataFrame):
    """Pull benchmark gains
    """
    earliest_date = open_positions["position_entry_date"].min()
    bm = yf.Ticker(DEFAULT_BENCHMARK_TICKER)
    bm_hist = bm.history(start=earliest_date)
    return bm_hist.reset_index()


@asset(partitions_def=DailyPartitionsDefinition(start_date="2023-10-01"))
def sell_recommendations(context: AssetExecutionContext, market_values: pd.DataFrame):
    """
    """
    pass