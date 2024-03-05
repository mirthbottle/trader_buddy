"""Test positions assets
"""

# from dagster import 
from unittest.mock import patch

from gain_tracker.resources.etrade_resource import ETrader
from gain_tracker.assets.positions import etrade_accounts, etrade_positions

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
    assert set(result.columns) == {'account_id', "account_id_key"}

@patch('gain_tracker.resources.etrade_api.ETradeAPI.create_authenticated_session')
def test_etrade_positions(mock_create_auth_sess):
    pass
