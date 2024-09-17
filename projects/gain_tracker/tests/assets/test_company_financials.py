"""Test company financials
"""

import pytest
import pandas as pd
from numpy import nan
from unittest.mock import patch, MagicMock

from gain_tracker.assets.company_financials import (
    get_total_revenue, get_total_revenues
)

@pytest.fixture
def yf_income_stmt():
    revs = pd.Series({
        pd.Timestamp('2023-12-31 00:00:00'): 176191000000.0, 
        pd.Timestamp('2022-12-31 00:00:00'): 158057000000.0, 
        pd.Timestamp('2021-12-31 00:00:00'): 136341000000.0, 
        pd.Timestamp('2020-12-31 00:00:00'): 127144000000.0, 
        pd.Timestamp('2019-12-31 00:00:00'): nan})
    stmt = pd.DataFrame({
        "Total Revenue": revs
    }).T
    yield stmt

@patch('yfinance.Ticker')
def test_get_total_revenue(mock_ticker, yf_income_stmt):
    """get revenue but mock out yfinance
    """
    revs = get_total_revenue('2z')
    assert len(revs) == 0
    
    co_tick = MagicMock()
    co_tick.income_stmt = yf_income_stmt
    mock_ticker.return_value = co_tick

    revs = get_total_revenue("F")
    print(revs)
    assert len(revs) == 4
    assert revs.loc['2023-12-31', "Total Revenue"] == 176191000000.0

def test_get_total_revenues():
    """Get a bunch of data and concat
    """
    tickers = pd.DataFrame({"Ticker": ["A", "B", "C"]})
    ar = get_total_revenues(tickers)
    print(ar)
    assert len(ar) > 0
    assert set(ar.columns) == {"Total Revenue", "year", "ticker"}