
import pytest
import pandas as pd
from datetime import date, datetime, timezone, timedelta
from dagster import build_asset_context
from unittest.mock import patch, MagicMock


from gain_tracker.assets.dividends import (
    attribute_dividend_to_positions, position_dividends
)

@pytest.fixture
def sample_etrade_positions():
    data = {
        "position_lot_id": [1, 2, 3],
        "symbol_description": ["GOOGL", "GOOGL", "MSFT"],
        "date": [date.fromisoformat("2025-01-30")]*3,
        "quantity": [10, 20, 20],
        "price_paid": [100]*3,
        "account_id_key": ["Hmz", "Hmz", "Hmz"],
    }
    yield pd.DataFrame(data)

@pytest.fixture
def sample_etrade_dividend_transactions():
    data = {
        "account_id": [1, 1],
        "account_id_key": ["Hmz", "Hmz"],
        "security_type": ["EQ"]*2,
        "symbol": ["GOOGL", "MSFT"],
        "quantity": [0]*2,
        "transaction_type": ["Dividend"]*2, 
        "transaction_date": [
            date.fromisoformat("2025-01-30")]*2,
        "fee": [0.01]*2, 
        "transaction_id": [25, 26],
        "amount": [3, 2.4],
        "timestamp": [date.fromisoformat("2025-01-30")]*2,
        "description": ["Dividend"]*2
    }
    yield pd.DataFrame(data)


def test_attribute_dividend_to_positions(
        sample_etrade_positions, sample_etrade_dividend_transactions):
    i_positions = sample_etrade_positions.set_index(
        ["account_id_key", "symbol_description"]
    )
    div_row = sample_etrade_dividend_transactions.rename(
                columns={
                    "transaction_type": "dividend_type",
                    "symbol": "symbol_description",
                    "amount": "dividend"}).iloc[0]
    div_row["account_id_key"] = "Hmz"
    result = attribute_dividend_to_positions(i_positions, div_row)
    print(result)
    # dividend is 3, first row has quantity 10 vs total of 30
    assert result.iloc[0]["dividend"] == 1
    assert result.iloc[1]["dividend"] == 2
    assert len(result) == 2
    assert all([q==30 for q in result["total_quantity"].values])

def test_position_dividends(
        sample_etrade_dividend_transactions,
        sample_etrade_positions
):
    context = build_asset_context(partition_key="2025-01-30")
    result = position_dividends(
        context, sample_etrade_dividend_transactions,
        sample_etrade_positions
    )
    df = list(result)[0].value
    print(df)
    assert len(df) == 3
    assert df.iloc[0]["dividend"] == 1
    assert df.iloc[1]["dividend"] == 2
    assert df.iloc[2]["dividend"] == 2.4