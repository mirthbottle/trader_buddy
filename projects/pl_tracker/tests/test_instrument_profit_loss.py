from datetime import datetime
from decimal import Decimal

from pl_tracker import instrument_profit_loss as ipl


def test_profit_loss():
    """
    profit loss percent * n_shares * start_price
    0.1*16*20 = 32

    it's the same as n_shares * (end_price - start_price)
    """
    result = ipl.profit_loss(
        Decimal("0.1"), 20, 16
    )

    assert result == Decimal("32")

def test_profit_loss_amortized_year():
    result = ipl.profit_loss_amortized_year(
        Decimal("120.6"), 730
    )
    assert result == Decimal("60.3")

    result = ipl.profit_loss_amortized_year(
        Decimal("-120.6"), 730
    )
    assert result == Decimal("-60.3")
    
    result = ipl.profit_loss_amortized_year(
        Decimal("0"), 730
    )
    assert result == Decimal("0")

def test_profit_loss_annualized_percent():
    start = datetime(2023, 6, 1)
    end = datetime(2023, 12, 1)

    result, days = ipl.profit_loss_annualized_percent(
        Decimal("0.1"), start, end)
    print(result)
    assert round(float(result), 4) == 0.2094

    result, days = ipl.profit_loss_annualized_percent(
        Decimal("0.03"), datetime(2023, 11, 30), end)
    print(result)
    assert round(float(result), 4) == 48481.7245

    result, days = ipl.profit_loss_annualized_percent(
        Decimal("0.1"), datetime(2018, 12,1), end)
    print(result)
    assert round(float(result), 5) == 0.01923

def test_profit_loss_percent():
    result = ipl.profit_loss_percent(4, 4.4)
    print(result)

    assert result == Decimal("0.1")