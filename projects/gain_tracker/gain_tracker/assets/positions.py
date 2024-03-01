"""

"""

from typing import Optional
import re
from datetime import date
import pandas as pd

from google.api_core.exceptions import NotFound
from dagster import (
    asset, multi_asset, AssetOut, AssetKey,
    DailyPartitionsDefinition, AssetExecutionContext
    )

import yfinance as yf

from ..resources.etrade_resource import ETrader

from .. import position_gain as pg
from ..position import Position

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
    snake_case = '_'.join(words).lower()
    return snake_case


@asset
def etrade_accounts(etrader: ETrader):
    """Pull accounts in etrade

    see if dagster can trigger opening a website and have a user input
    """
    accounts = pd.DataFrame(etrader.list_accounts())

    snake_cols = {c:camel_to_snake(c) for c in accounts.columns}
    accounts.rename(columns=snake_cols, inplace=True)
    return accounts

@asset
def etrade_positions(etrader: ETrader, etrade_accounts: pd.DataFrame):
    """Pull positions in etrade for each account
    """
    keys = etrade_accounts["account_id_key"].values

    all_positions = []
    for k in keys:
        print(k)
        portfolio = etrader.view_portfolio(k)
        print(portfolio)
        if portfolio is not None:
            ps = pd.DataFrame(portfolio)
            ps.loc[:, "accountIdKey"] = k
            all_positions.append(ps)

    positions = pd.concat(all_positions)
    print(positions.head())
    snake_cols = {c:camel_to_snake(c) for c in positions.columns}
    positions.rename(columns=snake_cols, inplace=True)

    pos_cols = [
        "symbol_description", "date_acquired", "price_paid", "quantity",
        "market_value", "account_id_key", "position_id"]
    # positions.set_index("position_id")
    return positions[pos_cols]


@multi_asset(
        outs={
            "open_positions": AssetOut(),
            "closed_positions": AssetOut()
        },
        can_subset=True
)
def updated_positions(
    etrade_positions: pd.DataFrame): 
    # etrade_transactions: Optional[pd.DataFrame]):
    """
    needs a bigquery resource
    some positions may have closed and are not in etrade_positions anymore

    positions gets updated

    use positionid
    not sure what happens to the positionid when only some shares are sold
    """
    from ..definitions import defs
    try:
        old_open_positions = defs.load_asset_value(AssetKey("open_positions")).set_index('position_id')
        old_closed_positions = defs.load_asset_value(AssetKey("closed_positions")).set_index('position_id')
    except NotFound:
        old_open_positions = pd.DataFrame([])
        old_closed_positions = pd.DataFrame([])

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

def make_position(r):
    """
    """
    p = Position(
        r["symbol_description"],
        position_entry_price=r["price_paid"],
        position_entry_date=r["date_acquired"],
        position_size=r["quantity"]
        )
    return p


@asset(
        partitions_def=DailyPartitionsDefinition(start_date="2023-10-01"),
        metadata={"partition_expr": "DATETIME(date)"}
)
def market_values(context: AssetExecutionContext, open_positions: pd.DataFrame):
    """Market values and gains
    
    save this asset daily? ok

    how to not overwrite closed positions, though
    may need a separate table for closed positions for their last values

    """
    partition_date_str = context.partition_key

    # positions = open_positions.apply(make_position, axis=1)
    # positions.apply(lambda p: p.recommend_exit_long())
    open_positions.loc[:, "market_price"] = open_positions.apply(
        lambda r: r["market_value"]/r["quantity"], axis=1
    )
    # need to get historical market prices from yahoo finance
    open_positions.loc[:, "percent_gain"] = open_positions.apply(
        lambda r: pg.compute_percent_price_gain(
            r["price_paid"], r["market_price"]), axis=1)

    open_positions.loc[:, "date"] = date.fromisoformat(partition_date_str)
    return open_positions[["date", "position_id", "symbol_description", "percent_gain"]]


@asset(partitions_def=DailyPartitionsDefinition(start_date="2023-10-01"),
       metadata={"partition_expr": "DATETIME(date)"}
       )
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