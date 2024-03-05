"""Test Etrader resource
which is an instance of EtradeAPI
"""
# import os
from unittest.mock import patch, MagicMock
from dagster import (DagsterInstance, build_init_resource_context)

from gain_tracker.resources.etrade_resource import ETrader

@patch('gain_tracker.resources.etrade_api.ETradeAPI.create_authenticated_session')
def test_etrader(mock_create_auth_sess):
    # os.environ["ENV"] = "dev"
    # os.environ["SESSION"]
    with DagsterInstance.ephemeral() as instance:
        context = build_init_resource_context(instance=instance)
        etrader = ETrader()
        # assert etrader.environment == "dev"
        et = etrader.create_resource(context)
        print(et.__dict__)
        assert et.environment == 'dev'