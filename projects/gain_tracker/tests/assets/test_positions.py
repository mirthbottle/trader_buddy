"""Test positions assets
"""

import pandas as pd
from datetime import datetime, timezone
from dagster import build_asset_context
from unittest.mock import patch

from gain_tracker.resources.etrade_resource import ETrader
from gain_tracker.assets.positions import (
    etrade_accounts, etrade_positions,
    positions_scd4,
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