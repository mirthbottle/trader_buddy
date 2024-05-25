"""Test positions assets
"""

import pytest
import pandas as pd
from datetime import date, datetime, timezone
from dagster import build_asset_context
from unittest.mock import patch, MagicMock

from gain_tracker.resources.etrade_resource import ETrader
from gain_tracker.assets.positions import (
    etrade_accounts, etrade_positions,
    positions_scd4, open_positions,
    gains)

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

    result = etrade_accounts(
        etrader=ETrader(session_token="xx", session_token_secret="sts")
    )
    print(result)
    assert len(result) == 2
    assert set(result.columns) == {'account_id', "account_id_key"
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

    etrade_accounts = pd.DataFrame({"account_id_key": ["acc_1"]})
    result = etrade_positions(
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
            datetime.fromisoformat("2021-09-01 10:00:00"), 
            datetime.fromisoformat("2021-09-01 11:00:00"), 
            datetime.fromisoformat("2021-09-01 12:00:00")]
    }
    yield pd.DataFrame(data)


@pytest.fixture
def sample_etrade_transactions():
    data = {
        "display_symbol": ["GOOGL", "MSFT"],
        "quantity": [2, -300],
        "transaction_type": ["Bought", "Sold"], 
        "transaction_date": [
            date.fromisoformat("2024-02-20"), date.fromisoformat("2024-03-26")],
        "fee": [0.01, 0.01], 
        "transaction_id": ["cc", "dd"],
        "amount": [-1000.0, 30000.0]
    }
    yield pd.DataFrame(data)

def test_positions_scd4_no_old_pos(
        sample_etrade_positions, sample_etrade_transactions):
    
    # no old_positions or old_positions_history
    # MSFT was sold
    pos, pos_history = positions_scd4(
        sample_etrade_positions, sample_etrade_transactions)
    print(pos.value)
    assert len(pos.value) == 3
    # doesn't know any are sold bc there are no old_open_positions
    print(pos_history.value)
    assert len(pos_history.value) == 3
    assert pos_history.value.loc[1, "change_type"] == "opened_position"

def test_positions_scd4_no_old_pos_bought(
        sample_etrade_positions, sample_etrade_transactions):

    # no old_positions or old_positions_history
    # MSFT was also bought
    sample_etrade_transactions.loc[1, "transaction_type"] = "Bought"
    pos, pos_history = positions_scd4(
        sample_etrade_positions, sample_etrade_transactions)
    assert len(pos.value) == 3
    # doesn't know any are sold bc there are no old_open_positions
    assert len(pos_history.value) == 3
    assert "transaction_fee" in pos_history.value.columns

@pytest.fixture
def sample_positions_history():
    data = {
        "symbol_description": ["AAPL", "GOOGL", "MSFT"],
        "date_acquired": ["2021-01-01", "2020-05-15", "2019-11-20"],
        "price_paid": [150.0, 2000.0, 120.0],
        "quantity": [100.0, 50.0, 300.0],
        "market_value": [17000.0, 100100.0, 34000.0], # changed from pos
        "original_qty": [100, 50, 300],
        "account_id_key": [101, 102, 103],
        "position_id": [1001, 1002, 1003],
        "position_lot_id": [1, 2, 3],
        "transaction_id": [None, None, None],
        "transaction_fee": [None, None, None],
        "change_type": ["opened_position", "opened_position", "opened_position"],
        "date_closed": [None, None, None],
        "timestamp": [
            datetime.fromisoformat("2021-08-01 10:00:00"), 
            datetime.fromisoformat("2021-08-01 11:00:00"), 
            datetime.fromisoformat("2021-08-01 12:00:00")],
        "time_updated": [
            datetime.fromisoformat("2021-08-01 10:00:00"), 
            datetime.fromisoformat("2021-08-01 11:00:00"), 
            datetime.fromisoformat("2021-08-01 12:00:00")]
    }
    yield pd.DataFrame(data)

@patch('gain_tracker.definitions.defs.load_asset_value')
def test_positions_scd4(
        mock_load_asset_value,
        sample_etrade_positions, sample_etrade_transactions,
        sample_positions_history):

    old_pos = sample_positions_history.drop("change_type", axis=1)

    pos_history_add_sold = sample_positions_history.copy(deep=True)
    pos_history_add_sold.loc[3] = pd.Series({
        "symbol_description": "GOOGL",
        "date_acquired": "2023-10-05",
        "price_paid": 200,
        "quantity": 4,
        "market_value": 800,
        "original_qty": 4,
        "account_id_key": 102,
        "position_id": 999,
        "position_lot_id": 0,
        "transaction_id": 500,
        "transaction_fee": 0.2,
        "change_type": "closed_position",
        "date_closed": date.fromisoformat("2023-08-01"),
        "timestamp": datetime.fromisoformat("2023-08-01 10:00:00"),
        "time_updated": datetime.fromisoformat("2023-08-01 10:00:00")
    })

    # patch in old_pos and old_pos_history
    mock_load_asset_value.side_effect=[
        old_pos.copy(deep=True), sample_positions_history.copy(deep=True),
        old_pos.copy(deep=True), pos_history_add_sold]

    pos, pos_history = positions_scd4(
        sample_etrade_positions.copy(deep=True), 
        sample_etrade_transactions.copy(deep=True))
    print(pos.value)
    assert len(pos.value) == 3
    # doesn't know any are sold bc there are no old_open_positions
    print(pos_history.value)
    assert len(pos_history.value) == 6

    # msft position is missing from new positions asset
    # but it's in the old_positions, passed by mock_load_asset_value
    pos_sold_msft = sample_etrade_positions.drop(2)
    pos, pos_history = positions_scd4(
        pos_sold_msft,
        sample_etrade_transactions.copy(deep=True))
    pos_history_sold = pos_history.value
    print(pos.value)
    assert len(pos.value) == 3
    # doesn't know any are sold bc there are no old_open_positions
    print(pos_history_sold)
    assert len(pos_history_sold) == 7
    assert len(pos_history_sold[
        pos_history_sold["change_type"]=="closed_position"]) == 2
    assert pos_history_sold.loc[6, "market_value"] == 30000
    assert pos_history_sold.loc[6, "transaction_fee"] == 0.01
    assert pos_history_sold.loc[6, "timestamp"] == datetime(2024, 3, 26, tzinfo=timezone.utc)

def test_gains():

    d = datetime(2024, 1, 1, tzinfo=timezone.utc).date()
    open_positions = pd.DataFrame(
        {
            "position_id": [0, 1, 2, 3],
            "position_lot_id": [10, 11, 12, 13],
            "symbol_description": ["A", "B", "C", "D"],
            "price_paid": [0.1, 1, 10, 3],
            "quantity": [1, 1, 2, 1],
            "market_value": [0.14, 1.1, 20, 3.3],
            "date_acquired": [d, d, d, d]
        }
    )
    context = build_asset_context(partition_key="2024-02-01")

    result = gains(context, open_positions)
    print(result)
    assert len(result) == 4
    assert "annualized_pct_gain" in result.columns