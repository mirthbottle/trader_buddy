import pytest
import pandas as pd
from unittest.mock import MagicMock
from datetime import date, datetime, timezone

from dagster import build_asset_context
from gain_tracker.assets import portfolios

# Import the assets to test

@pytest.fixture
def mock_etrader():
    etrader = MagicMock()
    etrader.get_account_balance.side_effect = lambda key: {
        "accountIdKey": key,
        "totalAccountValue": 1000.0 + int(key),
        "cashBalance": 100.0 + int(key)
    }
    return etrader

@pytest.fixture
def etrade_accounts_df():
    return pd.DataFrame({
        "account_id_key": ["1", "2"]
    })

def test_get_account_balances_returns_balances(mock_etrader):
    keys = ["1", "2"]
    df = portfolios.get_account_balances(keys, mock_etrader)
    assert not df.empty
    assert set(df['account_id_key']) == set(keys)
    assert 'total_account_value' in df.columns
    assert 'cash_balance' in df.columns

def test_get_account_balances_empty(mock_etrader):
    mock_etrader.get_account_balance.side_effect = None
    df = portfolios.get_account_balances(["1"], mock_etrader)
    assert df.empty

def test_portfolio_balances_asset(mock_etrader, etrade_accounts_df):
    context = build_asset_context(partition_key="2025-05-01")

    output = portfolios.portfolio_balances(
        context, mock_etrader, etrade_accounts_df)
    df = output.value
    assert not df.empty
    assert 'month' in df.columns
    assert 'timestamp' in df.columns
    assert set(df['account_id_key']) == set(["1", "2"])

def test_portfolio_gains_asset_empty():
    context = build_asset_context(partition_key="2025-05-01")
    # Should return empty DataFrame if balances are empty
    output = portfolios.portfolio_gains(context, pd.DataFrame([]), pd.DataFrame([]))
    assert output.value.empty

def test_portfolio_gains_asset(monkeypatch):
    # Patch compute_portfolio_gains to return a predictable DataFrame
    def fake_compute_portfolio_gains(balances, transfers):
        return pd.DataFrame({
            "account_id": ["1", "2"],
            "month": ["2024-04-01", "2024-05-01"],
            "percent_gain": [0.05, 0.10],
            "cumulative_transfers": [100, 200]
        })
    monkeypatch.setattr(portfolios, "compute_portfolio_gains", fake_compute_portfolio_gains)
    balances = pd.DataFrame({
        "account_id_key": ["1", "2"],
        "month": ["2024-04-01", "2024-05-01"]
    })
    transfers = pd.DataFrame({
        "account_id_key": ["1", "2"],
        "month": ["2024-04-01", "2024-05-01"],
        "amount": [100, 200]
    })
    context = build_asset_context(partition_key="2024-06-01")
    output = portfolios.portfolio_gains(context, balances, transfers)
    df = output.value
    print(df)
    assert "account_id" in df.columns
    assert "0m_percent_gain" not in df.columns  # months_ago 0
    assert "1m_transfers" in df.columns  # months_ago 1
    assert "2m_transfers" in df.columns  # months_ago 2
