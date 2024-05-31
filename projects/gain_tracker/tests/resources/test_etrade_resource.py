"""Test Etrader resource
which is an instance of EtradeAPI
"""
# import os
from unittest.mock import patch, MagicMock
from dagster import (DagsterInstance, build_init_resource_context)

from gain_tracker.resources.etrade_resource import ETrader

@patch('gain_tracker.resources.etrade_api.ETradeAPI.renew_access_token')
@patch('gain_tracker.resources.etrade_api.ETradeAPI.create_authenticated_session')
def test_etrader(mock_create_auth_sess, mock_renew_token):

    with DagsterInstance.ephemeral() as instance:
        context = build_init_resource_context(instance=instance)
        etrader = ETrader(session_token="xx", session_token_secret="sss")
        et = etrader.create_resource(context)
        print(et.__dict__)
        assert et.environment == 'dev'