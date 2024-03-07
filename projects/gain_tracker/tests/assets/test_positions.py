"""Test positions assets
"""

import pandas as pd
from datetime import datetime, timezone
from dagster import build_asset_context
from unittest.mock import patch

from gain_tracker.resources.etrade_resource import ETrader
from gain_tracker.assets.positions import (
    etrade_accounts, etrade_positions,
    market_values)

@patch('gain_tracker.resources.etrade_api.ETradeAPI.list_accounts')
@patch('gain_tracker.resources.etrade_api.ETradeAPI.create_authenticated_session')
def test_etrade_accounts(mock_create_auth_sess, mock_list_accounts):

    test_accounts = [
        {
            'accountId': '1',
            'accountIdKey': 'Hmz'},
        {
            'accountId': '2',
            'accountIdKey': 'Xab'}]
    mock_list_accounts.return_value = test_accounts

    result = etrade_accounts(
        etrader=ETrader()
    )
    print(result)
    assert len(result) == 2
    assert set(result.columns) == {'account_id', "account_id_key"
    }

@patch('gain_tracker.resources.etrade_api.ETradeAPI.create_authenticated_session')
def test_etrade_positions(mock_create_auth_sess):
    pass


def test_market_values():

    d = datetime(2024, 1, 1, tzinfo=timezone.utc).date()
    open_positions = pd.DataFrame(
        {
            "position_id": [0, 1, 2, 3],
            "symbol_description": ["A", "B", "C", "D"],
            "price_paid": [0.1, 1, 10, 3],
            "quantity": [1, 1, 2, 1],
            "market_value": [0.14, 1.1, 20, 3.3],
            "date_acquired": [d, d, d, d]
        }
    )
    context = build_asset_context(partition_key="2024-02-01")

    result = market_values(context, open_positions)
    print(result)
    assert len(result) == 4
    assert "annualized_pct_gain" in result.columns