"""Test positions assets
"""

import pytest
import pandas as pd
from datetime import date, datetime, timezone
from dagster import build_asset_context
from unittest.mock import patch, MagicMock

from gain_tracker.resources.etrade_resource import ETrader
from gain_tracker.assets.positions import (
    PT_INFO,
    etrade_accounts, etrade_positions, etrade_transactions,
    sold_transactions, closed_positions,
    gains, buy_recommendations_previously_sold, sell_recommendations)

TODAY_LOC = datetime.now(tz=PT_INFO).date()
TODAY_LOC_STR = TODAY_LOC.isoformat()
print(TODAY_LOC_STR)

@patch('gain_tracker.resources.etrade_api.ETradeAPI.list_accounts')
@patch('gain_tracker.resources.etrade_api.ETradeAPI.renew_access_token')
@patch('gain_tracker.resources.etrade_api.ETradeAPI.create_authenticated_session')
def test_etrade_accounts(
    mock_create_auth_sess, mock_renew_token, mock_list_accounts):

    test_accounts = [
        {
            'accountId': '1',
            'accountIdKey': 'Hmz'},
        {
            'accountId': '2',
            'accountIdKey': 'Xab'}]
    mock_list_accounts.return_value = test_accounts
    context = build_asset_context(partition_key=TODAY_LOC_STR)

    result = etrade_accounts(
        context,
        etrader=ETrader(session_token="xx", session_token_secret="sts")
    )
    print(result)
    assert len(result) == 2
    assert set(result.columns) == {'account_id', "account_id_key", "date"
    }

@patch('gain_tracker.resources.etrade_api.ETradeAPI.view_portfolio')
@patch('gain_tracker.resources.etrade_api.ETradeAPI.renew_access_token')
@patch('gain_tracker.resources.etrade_api.ETradeAPI.create_authenticated_session')
def test_etrade_positions(
    mock_create_auth_sess, mock_renew_token, mock_view_portfolio):
    test_portfolio = pd.DataFrame({
        "symbolDescription": ["AMD"],
        "dateAcquired": [1703048400000],
        "pricePaid": [90],
        "quantity": [8],
        "marketValue": [800],
        "originalQty": [8],
        "positionId": ["pid1"],
        "positionLotId": ["pid1_2"]
    })
    mock_view_portfolio.return_value = test_portfolio

    etrade_accounts = pd.DataFrame({
        "account_id_key": ["acc_1"], "date": TODAY_LOC})
    context = build_asset_context(partition_key=TODAY_LOC_STR)

    result = etrade_positions(
        context,
        etrader=ETrader(session_token="xx", session_token_secret="sts"),
        etrade_accounts=etrade_accounts
    )
    print(result)
    assert len(result) == 1

@pytest.fixture
def sample_etrade_positions():
    data = {
        "symbol_description": ["AAPL", "GOOGL", "MSFT"],
        "date_acquired": ["2021-01-01", "2020-05-15", "2019-11-20"],
        "price_paid": [150.0, 2000.0, 120.0],
        "quantity": [100.0, 50.0, 300.0],
        "market_value": [16000.0, 100000.0, 36000.0],
        "original_qty": [100, 50, 300],
        "account_id_key": [101, 102, 103],
        "position_id": [1001, 1002, 1003],
        "position_lot_id": [1, 2, 3],
        "timestamp": [
            datetime.fromisoformat("2021-09-01 11:00:00"), 
            datetime.fromisoformat("2021-09-01 11:00:00"), 
            datetime.fromisoformat("2021-09-01 02:00:00")]
    }
    yield pd.DataFrame(data)


@pytest.fixture
def sample_etrade_transactions():
    data = {
        "symbol": ["GOOGL", "MSFT"],
        "quantity": [2, -300],
        "transaction_type": ["Bought", "Sold"], 
        "transaction_date": [
            date.fromisoformat("2024-02-20"), date.fromisoformat("2024-03-26")],
        "fee": [0.01, 0.01], 
        "transaction_id": [24, 25],
        "amount": [-1000.0, 30000.0]
    }
    yield pd.DataFrame(data)

@pytest.fixture
def sample_sold_transactions():
    data = {
        "symbol": ["MSFT"],
        "quantity": [300],
        "transaction_type": ["Sold"], 
        "transaction_date": [
            date.fromisoformat("2024-03-26")],
        "fee": [0.01], 
        "transaction_id": [25],
        "amount": [30000.0]
    }
    yield pd.DataFrame(data)

def test_closed_positions(
        sample_etrade_positions, sample_sold_transactions):

    result = closed_positions(
        sample_sold_transactions.copy(deep=True),
        sample_etrade_positions.copy(deep=True), 
    )
    print(result)
    assert len(result) == 1
    assert result.loc[0, "market_value"] == 30000
    assert result.loc[0, "transaction_fee"] == 0.01
    assert result.loc[0, "timestamp"] == datetime(2024, 3, 26, tzinfo=timezone.utc)

    no_msft_pos = sample_etrade_positions.drop(2)
    result = closed_positions(
        sample_sold_transactions.copy(deep=True),
        no_msft_pos,
    )
    print(result)
    assert result is None

def test_gains():

    d = datetime(2024, 1, 1, tzinfo=timezone.utc).date()
    part_date = datetime(2024, 2, 1, tzinfo=timezone.utc).date()
    part_date_str = part_date.isoformat()

    positions = pd.DataFrame(
        {
            "position_id": [0, 1, 2, 3],
            "position_lot_id": [10, 11, 12, 13],
            "symbol_description": ["A", "B", "C", "D"],
            "price_paid": [0.1, 1, 10, 3],
            "quantity": [1, 1, 2, 1],
            "market_value": [0.14, 1.1, 20, 3.3],
            "date_acquired": 4*[d],
            "date": 4*[part_date]
        }
    )
    context = build_asset_context(partition_key=part_date_str)

    result = gains(context, positions)
    print(result)
    assert len(result) == 4
    assert "annualized_pct_gain" in result.columns