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
    sold_transactions,
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

def test_gains():

    d = datetime(2024, 1, 1, tzinfo=timezone.utc).date()
    part_date = datetime(2024, 2, 1, tzinfo=timezone.utc).date()
    part_date_str = part_date.isoformat()

    positions = pd.DataFrame(
        {
            "position_id": [0, 1, 2, 3],
            "position_lot_id": [10, 11, 12, 13],
            "account_id_key": 4*["IRA"],
            "symbol_description": ["A", "B", "C", "D"],
            "price_paid": [0.1, 1, 10, 3],
            "original_qty": [1, 1, 2, 1],
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