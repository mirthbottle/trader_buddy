import pytest
import pandas as pd
from decimal import Decimal, getcontext
from gain_tracker.portfolio import compute_portfolio_gains

getcontext().prec = 12

@pytest.fixture
def sample_portfolio_balances():
    return pd.DataFrame({
        'account_id': ['A', 'A', 'A', 'B', 'B', 'B'],
        'month': pd.to_datetime(['2024-01-01', '2024-02-01', '2024-03-01', '2024-01-01', '2024-02-01', '2024-03-01']).date,
        'total_account_value': [1000, 1200, 1300, 2000, 2100, 2050]
    })

@pytest.fixture
def sample_monthly_transfer_totals():
    """
    transfers 100 into account A in March
    transfers 100 into account B in January
    """
    return pd.DataFrame({
        'account_id': ['A', 'A', 'A', 'B', 'B', 'B'],
        'month': pd.to_datetime([
            '2024-01-01', '2024-02-01', '2024-03-01', 
            '2024-01-01', '2024-02-01', '2024-03-01']).date,
        'total_transfer_amount': [0, 0, 100, 100, 0, 0]
    })

def test_compute_portfolio_gains_basic(sample_portfolio_balances, sample_monthly_transfer_totals):
    """
    100 is transfered into A in March so all months' gains will have this transfer included
    100 is transfered into B in Jan so only Jan's gain will have this transfer included 
    
    expected results:
    For account A:
    Jan - March gain: ((1300-100) - 1000) / 1000 = 20%
    Feb - March gain: ((1300-100) - 1200) / 1200 = 0%
    no March gain as it's the latest month

    for account B:
    Jan - March gain: ((2050-100) - 2000) / 2000 = -2.5%
    Feb - March gain: (2050 - 2100) / 2100 = -2.38%
    no March gain as it's the latest month
    """
    
    result = compute_portfolio_gains(sample_portfolio_balances, sample_monthly_transfer_totals)

    print(result)
    # Should not include the latest month for each account
    assert all(result['month'] != pd.to_datetime('2024-03-01').date)
    
    # Should have current_account_value column
    assert 'current_account_value' in result.columns

    assert result.loc["A", "percent_gain"].values.tolist() == [0, Decimal("0.2")]
    assert result.loc["B", "percent_gain"].values.tolist() == \
        [Decimal("2050")/Decimal("2100")-Decimal("1.0"), Decimal("-0.025")]


def test_missing_transfers(sample_portfolio_balances, sample_monthly_transfer_totals):
    sample_monthly_transfer_totals.loc[: 'total_transfer_amount'] = 0
    result = compute_portfolio_gains(
        sample_portfolio_balances, sample_monthly_transfer_totals)

    # No transfers provided
    transfers = pd.DataFrame(columns=['account_id', 'month', 'total_transfer_amount'])
    result = compute_portfolio_gains(sample_portfolio_balances, transfers)
    assert 'current_account_value' in result.columns
    print(result)