"""

"""
import logging
from typing import Optional
import re
from datetime import date, datetime, timezone, timedelta
import pandas as pd
import pygsheets

from google.api_core.exceptions import NotFound
from dagster import (
    asset, multi_asset, Output, AssetOut, AssetKey,
    AssetExecutionContext, Config
    )

logger = logging.getLogger(__name__)

import yfinance as yf

from ..partitions import daily_partdef, weekly_partdef
from ..resources.etrade_resource import ETrader
from ..resources.gsheets_resource import GSheetsResource

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
    snake_case = '_'.join(words).lower()
    return snake_case


@asset
def etrade_accounts(etrader: ETrader):
    """Pull accounts in etrade

    see if dagster can trigger opening a website and have a user input
    """

    # etrader.create_authenticated_session(
    #     config.session_token, config.session_token_secret)
    accounts = pd.DataFrame(etrader.list_accounts())

    snake_cols = {c:camel_to_snake(c) for c in accounts.columns}
    accounts.rename(columns=snake_cols, inplace=True)
    return accounts

@asset(
        partitions_def=weekly_partdef,
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

    end_date_str = (partition_date + timedelta(days=7)).strftime("%m%d%Y")
    start_date_str = partition_date.strftime("%m%d%Y")
    keys = etrade_accounts["account_id_key"].values

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

        return transactions

@asset
def sold_transactions(
    etrade_transactions: pd.DataFrame
):
    """sold etrade_transactions
    """
    sold = etrade_transactions.loc[etrade_transactions["transaction_type"]=="Sold"]
    # cast quantity to float
    sold.loc[:, "quantity"] = sold["quantity"].apply(lambda s: -1.0*s)
    
    output_cols = [
        "symbol", "transaction_date", "price", "amount", "quantity",
        "fee", "account_id", "timestamp", "transaction_id"]
    return sold[output_cols].set_index("transaction_id")


@asset
def etrade_positions(etrader: ETrader, etrade_accounts: pd.DataFrame):
    """Pull positions in etrade for each account

    timestamp is generated
    """
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

    pos_cols = [
        "symbol_description", "date_acquired", "price_paid", "quantity",
        "market_value", "original_qty", "account_id_key", "position_id", "position_lot_id",
        "timestamp"]
    return positions[pos_cols]


@multi_asset(
        outs={
            "positions": AssetOut(),
            "positions_history": AssetOut(),
        },
)
def positions_scd4(
    etrade_positions: pd.DataFrame,
    etrade_transactions: pd.DataFrame,
):
    """SCD Type 4 positions and market_values

    positions has the latest
    positions_history adds a new line for changes

    timestamp is from input assets, keep in positions and positions_history
    market_values changes every day - output to separate table
    """
    from ..definitions import defs
    cols_to_compare = [
        "symbol_description", "date_acquired", "price_paid", "quantity",
        "account_id_key", "position_id", "original_qty", "market_value"]
    closing_cols = [
        "timestamp", "date_closed", "transaction_fee", "transaction_id"]
    
    try:
        old_positions = defs.load_asset_value(
            AssetKey("positions")).set_index('position_lot_id')
        old_positions_history = defs.load_asset_value(
            AssetKey("positions_history")).set_index('position_lot_id')
    except Exception as e: # NotFound:
        old_positions = pd.DataFrame(
            [], index=pd.Index([],name="position_lot_id", dtype='int64'),
            columns=cols_to_compare+closing_cols)
        old_positions_history = pd.DataFrame(
            [], index=pd.Index([],name="position_lot_id", dtype='int64'))

    etrade_positions = etrade_positions.set_index("position_lot_id").sort_index()
    
    # ignore ones that were previously closed
    old_open_positions = old_positions.loc[old_positions["date_closed"].isnull()]
    # compare the positions that are in both
    # join on position_lot_id
    positions_triage = etrade_positions.join(
        old_open_positions[["symbol_description"]], how="outer", rsuffix="_prev"
    )
    # in etrade_positions but not in old_positions
    new_open_positions = etrade_positions.loc[
        positions_triage[
            positions_triage["symbol_description_prev"].isnull()].index].copy()

    # start positions df
    positions = pd.concat([old_positions, new_open_positions])
    if len(new_open_positions)>0:
        new_open_positions.loc[:, "change_type"] = "opened_position"
        new_open_positions.loc[:, "time_updated"] = datetime.now(timezone.utc)
        
    in_both = positions_triage.loc[
        (~positions_triage[[
            "symbol_description", "symbol_description_prev"]].isna()).all(axis=1)]

    if len(old_positions) > 0:
        diff_positions = old_positions.loc[in_both.index, cols_to_compare].compare(
            etrade_positions.loc[in_both.index, cols_to_compare])
        updated_positions = etrade_positions.loc[diff_positions.index].copy()
        if len(updated_positions) > 0:
            updated_positions.loc[:, "change_type"] = "updated"
            updated_positions.loc[:, "time_updated"] = datetime.now(timezone.utc)
            positions.loc[updated_positions.index, cols_to_compare+["timestamp"]] = \
                updated_positions[cols_to_compare+["timestamp"]]
    else:
        updated_positions = pd.DataFrame([])    

    # select the positions that aren't in etrade_positions
    # and not already sold previously (date_closed is null)
    # if they're also in Sold transactions
    missing_old_positions = old_positions.loc[
        positions_triage.loc[
            positions_triage["symbol_description"].isnull()].index].copy()
    maybe_closed = missing_old_positions.loc[
        missing_old_positions["date_closed"].isnull()].reset_index().set_index(
            ['symbol_description', 'quantity'])
    # TODO: also need to add positions where quantity decreased
    sold = etrade_transactions.loc[etrade_transactions["transaction_type"]=="Sold"]
    # cast quantity to float
    sold.loc[:, "quantity"] = sold["quantity"].apply(lambda s: -1.0*s)

    # use the market_value from the transactions
    closed_transactions = (
        sold
        .rename(columns={
            "symbol": "symbol_description",
            "fee": "transaction_fee", "amount": "market_value",
            "transaction_date": "date_closed"})
        .set_index(["symbol_description", "quantity"])
    )
    closed_transactions.loc[:, "timestamp"] = closed_transactions[
        "date_closed"].apply(
            lambda d: datetime.combine(d, datetime.min.timetz(), tzinfo=timezone.utc))

    # transactions data takes precedence over data from the positions table
    new_closed_positions = pd.merge(
        maybe_closed, # should have position_lot_id column
        closed_transactions[closing_cols+["market_value"]],
        left_index=True, right_index=True,
        suffixes=("_pos", None)).reset_index().set_index("position_lot_id")
    print(new_closed_positions)
    if len(new_closed_positions)>0:
        new_closed_positions.loc[:, "change_type"] = "closed_position"
        new_closed_positions.loc[:, "time_updated"] = datetime.now(timezone.utc)
        positions.loc[new_closed_positions.index, closing_cols] = \
            new_closed_positions[closing_cols]

    yield Output(
        positions[cols_to_compare+closing_cols].reset_index(), output_name="positions")
    
    history_cols = cols_to_compare+closing_cols+["change_type", "time_updated"]
    positions_history = pd.concat([
        old_positions_history, new_open_positions, updated_positions,
        new_closed_positions]).reindex(
            columns=history_cols
        ).reset_index()
    yield Output(
        positions_history, output_name="positions_history")

@asset
def open_positions(positions: pd.DataFrame):
    """Filter positions
    """
    open_ps = positions.loc[positions["date_closed"].isnull()]
    
    return open_ps

@asset
def closed_positions(positions: pd.DataFrame):
    """Filter positions
    """
    closed_ps = positions.loc[~positions["date_closed"].isnull()]
    return closed_ps

@asset(
        partitions_def=daily_partdef,
        metadata={"partition_expr": "DATETIME(date)"}
)
def gains(context: AssetExecutionContext, open_positions: pd.DataFrame):
    """Market values and gains
    
    save this asset daily? ok

    how to not overwrite closed positions, though
    may need a separate table for closed positions for their last values

    """
    partition_date_str = context.partition_key
    partition_date = date.fromisoformat(partition_date_str)

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


@asset(
        partitions_def=daily_partdef,
        metadata={"partition_expr": "DATETIME(date)"}
)
def benchmark_values(context: AssetExecutionContext, open_positions:pd.DataFrame):
    """Pull benchmark gains
    """
    earliest_date = open_positions["date_acquired"].min()
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
        metadata={"partition_expr": "DATETIME(date)"}
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
    sold.rename(columns={"price": "price_sold"}, inplace=True)

    sold.loc[:, "market_price"] = \
        sold["symbol"].apply(get_current_price_yf)
    
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

