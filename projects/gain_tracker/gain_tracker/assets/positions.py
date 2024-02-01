"""

"""

import pandas as pd
from dagster import asset

import yfinance as yf

DEFAULT_BENCHMARK_TICKER="IVV"

@asset
def benchmarks(positions:pd.DataFrame):
    """Pull benchmark gains
    """
    earliest_date = positions["position_entry_date"].min()
    bm = yf.Ticker(DEFAULT_BENCHMARK_TICKER)
    bm_hist = bm.history(start=earliest_date)
    # return bm_hist.reset_index()
