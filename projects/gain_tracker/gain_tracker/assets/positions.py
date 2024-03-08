"""

"""

import logging
from typing import Optional
import re
from datetime import date, datetime, timezone
import pandas as pd

from google.api_core.exceptions import NotFound
from dagster import (
    asset, multi_asset, Output, AssetOut, AssetKey,
    DailyPartitionsDefinition, AssetExecutionContext
    )

logger = logging.getLogger(__name__)

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
        logger.info(k)
        portfolio = etrader.view_portfolio(k)
        logger.debug(portfolio)
        if portfolio is not None:
            # ps = pd.DataFrame(portfolio)
            portfolio.loc[:, "accountIdKey"] = k
            all_positions.append(portfolio)

    positions = pd.concat(all_positions)
    logger.debug(positions.head())
    snake_cols = {c:camel_to_snake(c) for c in positions.columns}
    positions.rename(columns=snake_cols, inplace=True)
    positions.loc[:, "timestamp"] = datetime.now(timezone.utc)
    positions.loc[:, "date_acquired"] = positions["date_acquired"].apply(
        lambda d: datetime.fromtimestamp(d/1000).date())

    pos_cols = [
        "symbol_description", "date_acquired", "price_paid", "quantity",
        "market_value", "original_qty", "account_id_key", "position_id", "position_lot_id",
        "timestamp"]
    
    return positions[pos_cols]


@multi_asset(
        outs={
            "open_positions": AssetOut(),
            "closed_positions": AssetOut(is_required=False)
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

    use position_lot_id
    not sure what happens to the position_lot_id when only some shares are sold
    """
    from ..definitions import defs
    try:
        old_open_positions = defs.load_asset_value(
            AssetKey("open_positions")).set_index('position_lot_id')
    except NotFound:
        old_open_positions = pd.DataFrame(
            [], index=pd.Index([],name="position_lot_id"))

    try:
        old_closed_positions = defs.load_asset_value(
            AssetKey("closed_positions")).set_index('position_lot_id')
    except NotFound:
        old_closed_positions = pd.DataFrame(
            [], index=pd.Index([],name="position_lot_id"))

    etrade_positions.set_index("position_lot_id", inplace=True)
    # select the positions that aren't in etrade_positions?
    new_closed_positions = old_open_positions.loc[
        ~old_open_positions.index.isin(etrade_positions.index)]#.copy()
    closed_positions = pd.concat([old_closed_positions, new_closed_positions])

    yield Output(etrade_positions, output_name="open_positions")
    
    if len(closed_positions)>0:
        yield Output(closed_positions, output_name="closed_positions")

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
        partitions_def=DailyPartitionsDefinition(start_date="2023-10-01", end_offset=1),
        metadata={"partition_expr": "DATETIME(date)"}
)
def market_values(context: AssetExecutionContext, open_positions: pd.DataFrame):
    """Market values and gains
    
    save this asset daily? ok

    how to not overwrite closed positions, though
    may need a separate table for closed positions for their last values

    """
    partition_date_str = context.partition_key
    partition_date = date.fromisoformat(partition_date_str)

    # positions = open_positions.apply(make_position, axis=1)
    open_positions.loc[:, "market_price"] = open_positions.apply(
        lambda r: r["market_value"]/r["quantity"], axis=1
    )
    # need to get historical market prices from yahoo finance
    open_positions.loc[:, "percent_price_gain"] = open_positions.apply(
        lambda r: pg.compute_percent_price_gain(
            r["price_paid"], r["market_price"]), axis=1)
    open_positions.loc[:, "gain"] = open_positions.apply(
        lambda r: pg.compute_gain(r["percent_price_gain"], r["quantity"], r["price_paid"]),
        axis=1
    )
    open_positions.loc[:, "percent_gain"] = open_positions.apply(
        lambda r: pg.compute_percent_gain(r["gain"], r["quantity"], r["price_paid"]),
        axis=1
    )
    open_positions[["annualized_pct_gain", "days_held"]] = open_positions.apply(
        lambda r: pg.compute_annualized_percent_gain(
            r["percent_gain"], r["date_acquired"], partition_date
        ),
        axis=1, result_type="expand"
    )

    open_positions.loc[:, "date"] = partition_date
    return open_positions[[
        "date", "position_id", "position_lot_id", "symbol_description", "market_price", "percent_price_gain",
        "gain", "percent_gain", "annualized_pct_gain", "days_held"]]


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