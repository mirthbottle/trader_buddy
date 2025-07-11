import pytest
import pandas as pd

from datetime import date, datetime

from dagster import build_asset_context

from gain_tracker.assets.transfers import (
    transfer_transactions, monthly_transfer_totals
)

@pytest.fixture
def sample_transactions():
    """Sample DataFrame for transfer transactions.
    2 accounts: A1 and B1
    3 transactions
    """
    yield pd.DataFrame({
        'transaction_id': ['1', '2', '3'],
        'transaction_date': [
            pd.Timestamp('2025-01-01'), 
            pd.Timestamp('2025-01-01'), pd.Timestamp('2025-01-23')],
        'transaction_type': ['Transfer', 'Online Transfer', 'Contribution'],
        'description': ['Transfer from A', 'Transfer to B', 'Contribution to A'],
        'amount': [-200, 200, 50],
        'fee': [0, 0, 0],
        'account_id': ['A1', 'B1', 'A1'],
        'timestamp': [
            pd.Timestamp('2025-01-01 10:00:00'), pd.Timestamp('2025-01-02 11:00:00'), 
            pd.Timestamp('2025-01-03 12:00:00')]
    })

def test_transfer_transactions(sample_transactions):

    result = transfer_transactions(
        build_asset_context(partition_key="2025-01-01"),
        etrade_transactions=sample_transactions
    )

    output = list(result)[0]
    result = output.value
    print(result)
    assert len(result) == 3
    print(result.to_dict(orient='list'))

def test_monthly_transfer_totals(sample_transactions):
    """Test monthly transfer totals aggregation."""
    context = build_asset_context(partition_key="2025-01-01")
    
    result = monthly_transfer_totals(
        context,
        transfer_transactions=sample_transactions
    )

    output = list(result)[0]
    result = output.value
    print(result)
    
    assert len(result) == 2  # Two accounts A1 and B1
    assert set(result['account_id']) == {'A1', 'B1'}
    indexed = result.set_index("account_id")
    assert indexed.loc["A1", 'total_transfer_amount'] == -150  # -200 + 50 = 150  
