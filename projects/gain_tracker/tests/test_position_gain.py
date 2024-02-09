from datetime import datetime
from decimal import Decimal

from ..gain_tracker import position_gain as pg


def test_compute_gain():
    """
    percent price gain * n_shares * start_price
    0.1*16*20 = 32

    it's the same as n_shares * (end_price - start_price)
    """
    result = pg.compute_gain(
        Decimal("0.1"), 20, 16
    )

    assert result == Decimal("32")

def test_compute_annualized_gain():
    """
    compute annualized gain from raw gain and days position is held
    """
    result = pg.compute_annualized_gain(
        Decimal("120.6"), 730
    )
    assert result == Decimal("60.3")

    result = pg.compute_annualized_gain(
        Decimal("-120.6"), 730
    )
    assert result == Decimal("-60.3")
    
    result = pg.compute_annualized_gain(
        Decimal("0"), 730
    )
    assert result == Decimal("0")

def test_compute_annualized_percent_price_gain():
    """
    input percent gain

    test for different timefrmes:
    1. half a year
    2. less than 1 month
    3. more than 1 year
    """
    start = datetime(2023, 6, 1)
    end = datetime(2023, 12, 1)

    result, days = pg.compute_annualized_percent_gain(
        Decimal("0.1"), start, end)
    print(result)
    assert round(float(result), 4) == 0.2094
    assert days > 180

    result, days = pg.compute_annualized_percent_gain(
        Decimal("0.03"), datetime(2023, 11, 30), end)
    print(result)
    assert round(float(result), 4) == 48481.7245

    result, days = pg.compute_annualized_percent_gain(
        Decimal("0.1"), datetime(2018, 12,1), end)
    print(result)
    assert round(float(result), 5) == 0.01923

def test_compute_percent_price_gain():
    """
    test that float to decimal is computed correctly
    """
    result = pg.compute_percent_price_gain(4, 4.4)
    print(result)

    assert result == Decimal("0.1")