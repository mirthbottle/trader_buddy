"""
gain_tracker.assets.positions
"""
import logging
from typing import Optional
import re
from datetime import date, datetime, timezone, timedelta
from dateutil.relativedelta import relativedelta

import pandas as pd
import pygsheets
import pyarrow as pa

from google.api_core.exceptions import NotFound
from dagster import (
    asset, AssetIn, Output, LastPartitionMapping,
    # multi_asset, Output, AssetOut, AssetKey,
    TimeWindowPartitionMapping, AllPartitionMapping,
    AssetExecutionContext, Config,
    )

logger = logging.getLogger(__name__)

import yfinance as yf

from ..partitions import daily_partdef, monthly_partdef
from ..resources.etrade_resource import ETrader
from ..resources.gsheets_resource import GSheetsResource

from .. import position_gain as pg
from ..position import ClosedPosition, OpenPosition
from . import PT_INFO

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


@asset(
        output_required=False,
        metadata={"partition_expr": "DATETIME(date)"},
)
def etrade_accounts(context: AssetExecutionContext, etrader: ETrader):
    """Pull accounts in etrade

    see if dagster can trigger opening a website and have a user input

    check if today local time is still partition_date

    actually this could be a SCD
    """
    partition_date_str = context.partition_key
    partition_date = date.fromisoformat(partition_date_str)
    
    today_loc = datetime.now(tz=PT_INFO).date()
    logger.info(today_loc)

    if today_loc != partition_date:
        raise ValueError(f"today {today_loc} is not {partition_date_str}")
    # etrader.create_authenticated_session(
    #     config.session_token, config.session_token_secret)
    accounts = pd.DataFrame(etrader.list_accounts())

    snake_cols = {c:camel_to_snake(c) for c in accounts.columns}
    accounts.rename(columns=snake_cols, inplace=True)
    accounts.loc[:, "date"] = partition_date

    return accounts

def get_transactions(keys, start_date_str, end_date_str, etrader):
    all_transactions = []
    for k in keys:
        logger.info(k)
        ts = etrader.get_transactions(k, date_range=(start_date_str, end_date_str))
        if ts is not None:
            ts.loc[:, "accountIdKey"] = k
            all_transactions.append(ts)

    if len(all_transactions) > 0:
        transactions = pd.concat(all_transactions)
        snake_cols = {c: camel_to_snake(c) for c in transactions.columns}
        transactions.rename(columns=snake_cols, inplace=True)
        transactions.loc[:, "transaction_id"] = transactions["transaction_id"].astype("int64")
        transactions.loc[:, "timestamp"] = datetime.now(timezone.utc)
        transactions.loc[:, "transaction_date"] = transactions["transaction_date"].apply(
            lambda d: datetime.fromtimestamp(d/1000).date())
        transactions.drop_duplicates(subset=["transaction_id"], inplace=True)
        return transactions
    else:
        return []

@asset(
        partitions_def=daily_partdef,
        metadata={"partition_expr": "DATETIME(transaction_date)"},
        output_required=False
)
def etrade_transactions(
    context: AssetExecutionContext, etrader: ETrader, etrade_accounts: pd.DataFrame):
    """Get latest transactions

    try weekly where the partition_date is the 
    """
    partition_date_str = context.partition_key
    partition_date = date.fromisoformat(partition_date_str)

    end_date_str = (partition_date + timedelta(days=1)).strftime("%m%d%Y")
    start_date_str = partition_date.strftime("%m%d%Y")
    keys = etrade_accounts["account_id_key"].unique()
    logger.info(f'dates: {start_date_str} to {end_date_str}')
    transactions = get_transactions(keys, start_date_str, end_date_str, etrader)

    if len(transactions) > 0:
        yield Output(transactions)

@asset(
        partitions_def=monthly_partdef,
        metadata={
            "partition_expr": "DATETIME(transaction_date)"},
        ins={
            "etrade_accounts": AssetIn(
                partition_mapping=TimeWindowPartitionMapping(
                    allow_nonexistent_upstream_partitions=True
                )
            )
        },
        output_required=False
)
def etrade_monthly_transactions(
    context: AssetExecutionContext, etrader: ETrader, etrade_accounts: pd.DataFrame):
    """Get latest transactions

    start date is the partition_date
    end date is the end of the month
    """
    partition_date_str = context.partition_key
    partition_date = date.fromisoformat(partition_date_str)

    end_date_str = (partition_date + relativedelta(months=1)).strftime("%m%d%Y")
    start_date_str = partition_date.strftime("%m%d%Y")
    keys = etrade_accounts["account_id_key"].unique()
    logger.info(f'dates: {start_date_str} to {end_date_str}')
    transactions = get_transactions(keys, start_date_str, end_date_str, etrader)
    
    if len(transactions) > 0:
        yield Output(transactions)


@asset(
        partitions_def=daily_partdef,
        metadata={"partition_expr": "DATETIME(transaction_date)"},
        output_required=False
)
def sold_transactions(
    context: AssetExecutionContext,
    etrade_transactions: pd.DataFrame
):
    """sold etrade_transactions
    """
    sold = etrade_transactions.loc[
        etrade_transactions["transaction_type"]=="Sold"].copy(deep=True)
    # cast quantity to float
    sold.loc[:, "quantity"] = sold["quantity"].apply(lambda s: -1.0*s)
    
    output_cols = [
        "transaction_id", "symbol", "transaction_date", "price", "amount", "quantity",
        "fee", "account_id", "timestamp"]
    
    if len(sold) > 0:
        yield Output(sold[output_cols])


@asset(
        output_required=False,
        partitions_def=daily_partdef,
        metadata={"partition_expr": "DATETIME(date)"}
)
def etrade_positions(
    context: AssetExecutionContext, etrader: ETrader, etrade_accounts: pd.DataFrame):
    """Pull positions in etrade for each account

    timestamp is generated

    check is today_utc is the partition_date
    """
    partition_date_str = context.partition_key
    partition_date = date.fromisoformat(partition_date_str)
    
    today_loc = datetime.now(tz=PT_INFO).date()
    logger.info(today_loc)

    if today_loc != partition_date:
        raise ValueError(f"today {today_loc} is not {partition_date_str}")

    keys = etrade_accounts["account_id_key"].values

    all_positions = []
    for k in keys:
        logger.info(k)
        portfolio = etrader.view_portfolio(k)
        logger.debug(portfolio)
        if portfolio is not None:
            portfolio.loc[:, "accountIdKey"] = k
            all_positions.append(portfolio)

    positions = pd.concat(all_positions)
    logger.debug(positions.head())
    snake_cols = {c:camel_to_snake(c) for c in positions.columns}
    positions.rename(columns=snake_cols, inplace=True)
    positions.loc[:, "timestamp"] = datetime.now(timezone.utc)
    positions.loc[:, "date_acquired"] = positions["date_acquired"].apply(
        lambda d: datetime.fromtimestamp(d/1000).date())
    positions.loc[:, "date"] = partition_date

    pos_cols = [
        "symbol_description", "date", "date_acquired", "price_paid", "quantity",
        "market_value", "original_qty", "account_id_key", "position_id", "position_lot_id",
        "timestamp"]
    return positions[pos_cols]


@asset(
        partitions_def=daily_partdef,
        metadata={"partition_expr": "DATETIME(date)"}
)
def gains(context: AssetExecutionContext, etrade_positions: pd.DataFrame):
    """Market values and gains
    
    save this asset daily? ok

    how to not overwrite closed positions, though
    may need a separate table for closed positions for their last values

    """
    partition_date_str = context.partition_key
    partition_date = date.fromisoformat(partition_date_str)

    positions = etrade_positions.apply(
        lambda r: OpenPosition(
            r["position_lot_id"],
            r["account_id_key"],
            r["symbol_description"],
            r["price_paid"],
            r["date_acquired"],
            r["quantity"],
            r["original_qty"],
            r["market_value"]), axis=1)
    
    gmetrics = positions.apply(lambda p: p.compute_gains(partition_date))

    gm_df = pd.DataFrame(gmetrics.values.tolist())
    etrade_positions.loc[:, "market_price"] = etrade_positions.apply(
        lambda r: r["market_value"]/r["quantity"], axis=1
    )

    gains_df = pd.concat([
        etrade_positions.reset_index(drop=True),gm_df],axis=1)
    
    # print(gains_df.values)
    # gain_cols = ["percent_price_gain", "gain", "percent_gain", "annualized_pct_gain"]
    # gains_df[gain_cols] = gains_df[gain_cols].astype(
    #     pd.ArrowDtype(pa.decimal256(76, 40)))

    return gains_df[[
        "date", "position_id", "position_lot_id", "symbol_description", "market_price", "percent_price_gain",
        "gain", "percent_gain", "annualized_pct_gain", "days_held"]]


@asset(
        partitions_def=daily_partdef,
        metadata={"partition_expr": "DATETIME(date)"}
)
def benchmark_values(context: AssetExecutionContext, etrade_positions:pd.DataFrame):
    """Pull benchmark gains
    """
    earliest_date = etrade_positions["date_acquired"].min()
    bm = yf.Ticker(DEFAULT_BENCHMARK_TICKER)
    bm_hist = bm.history(start=earliest_date)
    return bm_hist.reset_index()

class BuyRecPrevSoldConfig(Config):
    """To customize the buy_recommendations_previously_sold asset

    min_dip_percent - float between 0-1 for the % dip in market price
    """
    min_dip_percent: float = 0.1

def get_current_price_yf(ticker:str):
    """Use Yahoo finance to retrieve current price

    ticker is passed as a str. works for US markets
    """
    try:
        price = yf.Ticker(ticker).fast_info["lastPrice"]
    except Exception as e:
        print(f"trouble getting current price for {ticker}: {e}")
        return None
    return price

@asset(
        partitions_def=daily_partdef,
        metadata={"partition_expr": "DATETIME(date)"},
        ins={
            "sold_transactions": AssetIn(
                partition_mapping=AllPartitionMapping(),
            )
        }
)
def buy_recommendations_previously_sold(
    context: AssetExecutionContext, config: BuyRecPrevSoldConfig,
    sold_transactions: pd.DataFrame):
    """Monitor dips in price from previously sold positions

    pull price from Etrade or yahoo finance
    there will be one rec per day, ok

    ops:
      buy_recommendations_previously_sold:
        config:
          min_dip_percent: 0.1
    """
    partition_date_str = context.partition_key
    partition_date = date.fromisoformat(partition_date_str)

    sold = sold_transactions.copy()
    sold_symbols = sold_transactions.drop_duplicates(subset=['symbol'])
    sold.rename(columns={"price": "price_sold"}, inplace=True)

    sold_symbols.loc[:, "market_price"] = \
        sold_symbols["symbol"].apply(get_current_price_yf)
    sold = pd.merge(
        sold, sold_symbols[["symbol", "market_price"]], on="symbol", how="left")

    # the time that the price was retrieved
    # but it may be after market is closed
    # so it's not the same as the time of the market_price
    sold.loc[:, "timestamp"] = datetime.now(timezone.utc)
    
    sold.loc[:, "percent_price_gain"] = sold.apply(
        lambda r: pg.compute_percent_price_gain(
            r["price_sold"], r["market_price"]), axis=1
    )
    
    sold.loc[:, "recommend_buy"] = sold["percent_price_gain"].apply(
        lambda p: p <= -1*config.min_dip_percent
    )
    sold.loc[:, "date"] = partition_date

    output_cols = [
        "date", "symbol", "transaction_date", "price_sold", 
        "market_price", "percent_price_gain", "quantity",
        "recommend_buy", "account_id", "timestamp", "transaction_id"]
    return sold[output_cols].set_index("transaction_id")

@asset(
        partitions_def=daily_partdef,
        metadata={"partition_expr": "DATETIME(date)"})
def sell_recommendations(context: AssetExecutionContext, gains: pd.DataFrame):
    """Recommend positions to sell

    could add AI here
    """
    market_rate = 0.25
    min_gain = 25
    min_percent_gain = 0.07

    recs = gains.copy()

    recs.loc[:, "pass_sell_filters"] = recs.apply(
        lambda r: len(
            [p for p in [
                pg.greater_than_eq(r["annualized_pct_gain"],market_rate),
                pg.greater_than_eq(r["gain"], min_gain),
                pg.greater_than_eq(r["percent_gain"], min_percent_gain)] if p]),
        axis=1
    )
    # print(recs.loc[recs["pass_sell_filters"]>0].sort_values(
    #     by="annualized_pct_gain", ascending=False).head(15))
    return recs

def date_to_str(d):
    if pd.notnull(d):
        return d.isoformat()
    else:
        return ""

def type_to_str(d):
    if pd.notnull(d):
        return str(d)
    else:
        return ""


@asset(
        partitions_def=daily_partdef,
        metadata={"partition_expr": "DATETIME(date)"}
)
def all_recommendations(
    context: AssetExecutionContext, 
    gsheets: GSheetsResource,
    sell_recommendations: pd.DataFrame,
    buy_recommendations_previously_sold: pd.DataFrame,
):
    """Output to gsheets
    
    inputs already have the `date` column
    """
    output_cols = [
        "date", "position_lot_id", "symbol",
        "days_held", "market_price", 
        "gain", "percent_price_gain", "annualized_pct_gain", 
        "date_sold", "price_sold", "recommendation"]

    sell_recommendations.loc[:, "recommendation"] = sell_recommendations[
        "pass_sell_filters"].apply(
            lambda f: "SELL" if f >=2 else "")
    buy_recommendations_previously_sold.loc[:, "recommendation"] = buy_recommendations_previously_sold[
        "recommend_buy"].apply(
            lambda r: "BUY" if r else ""
        )
    sell_colmap = {"symbol_description": "symbol"}
    buy_colmap = {"transaction_date": "date_sold"}
    recs = pd.concat([
        sell_recommendations.rename(columns=sell_colmap).sort_values(
            by="annualized_pct_gain", ascending=False
        ), 
        buy_recommendations_previously_sold.rename(columns=buy_colmap)])[
            output_cols]
    
    recs_gs = recs.copy(deep=True)
    recs_gs["date"] = recs_gs["date"].apply(date_to_str)
    recs_gs["date_sold"] = recs_gs["date_sold"].apply(date_to_str)
    for col in output_cols:
        recs_gs[col] = recs_gs[col].apply(type_to_str)
    

    sheet_key = "18WrLUfVqPcK-N33rnCKIHO4NmjkljDS1eNKA65shkRQ"
    
    wks = gsheets.open_sheet_first_tab(sheet_key)
    wks.clear()
    wks.title = "Recommendations"
    wks.set_dataframe(recs_gs, (1, 1))

    context.add_output_metadata({"row_count": len(recs)})

    pct_cell = pygsheets.Cell("G2")
    pct_cell.set_number_format(
        format_type=pygsheets.FormatType.PERCENT,
        pattern="0.00%"
    )

    price_cell = pygsheets.Cell("E2")
    price_cell.set_number_format(
        format_type=pygsheets.FormatType.CURRENCY
    )

    last_row = len(recs)+1
    print(last_row)
    pygsheets.DataRange(
        "G2", f"G{last_row}", worksheet=wks).apply_format(pct_cell)

    pygsheets.DataRange(
        "E2", f"E{last_row}", worksheet=wks).apply_format(price_cell)
    
    pygsheets.DataRange(
        "K{len(sell_recommendations)+1}", 
        f"K{last_row}", worksheet=wks).apply_format(price_cell)
    
    return recs

