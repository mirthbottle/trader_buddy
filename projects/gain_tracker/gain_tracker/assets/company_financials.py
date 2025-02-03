"""Pull financial data

"""

import logging
from typing import Optional
import re
from datetime import date, datetime, timezone, timedelta
from zoneinfo import ZoneInfo
import pandas as pd

from dagster import (
    asset, AssetIn, TimeWindowPartitionMapping,
    # multi_asset, Output, AssetOut, AssetKey,
    AllPartitionMapping,
    AssetExecutionContext, Config,
    )

logger = logging.getLogger(__name__)

import yfinance as yf


def get_total_revenue(ticker:str):
    """retrieve data for one ticker
    """
    co_ticker = yf.Ticker(ticker)
    income_stmt = co_ticker.income_stmt
    if len(income_stmt) == 0:
        logger.warning(
            f"{ticker} with shortName {co_ticker.info.get('shortName')} has no income_statement")
        return pd.DataFrame([])
    revs = co_ticker.income_stmt.loc["Total Revenue"].to_frame()
    revs.dropna(inplace=True)
    revs.index.name = "statement_date"
    revs.loc[:, "year"] = revs.apply(lambda r: r.name.year, axis=1)
    revs.loc[:, "ticker"] = ticker
    return revs

def get_total_revenues(tickers: pd.DataFrame, ticker_colname:str="Ticker"):
    """retrieve total revenues

    do each row
    """
    # tickers_str = " ".join(tickers[ticker_colname].tolist())
    
    revenues = tickers[ticker_colname].apply(get_total_revenue)
    all_revenues = pd.concat(revenues.values)
    return all_revenues

@asset(
        ins={
            "etrade_positions": AssetIn(
                partition_mapping=AllPartitionMapping()),
            "closed_positions": AssetIn(
                partition_mapping=AllPartitionMapping())
        }
)
def total_revenue(
    etrade_positions: pd.DataFrame, closed_positions: pd.DataFrame):
    """Pull total revenues of open_positions"""
    all_positions = pd.concat(
        [etrade_positions, closed_positions]
    ).drop_duplicates(subset=["symbol_description"])

    all_revs = get_total_revenues(
        all_positions[["symbol_description"]], ticker_colname="symbol_description")
    return all_revs

