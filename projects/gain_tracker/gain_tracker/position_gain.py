"""Compute unrealized gains and losses

# change terminology to be gain
"""
from typing import Optional
import logging
from datetime import datetime
from decimal import Decimal, getcontext
getcontext().prec = 12

logger = logging.getLogger(__name__)

def greater_than_eq(a, b):
    """a >= b
    """
    try:
        return a >= b
    except Exception as e:
        print(e)
        return False

def compute_percent_price_gain(
        start_price: float, end_price: float
        ) -> Decimal:
    """Compute gain of price only

    scale -1 to 1
    eg. 0.1 = 10%

    this doesn't include transactions...
    should I be computing the raw gain first instead?
    """
    pnlp = Decimal(str(end_price))/Decimal(str(start_price))+Decimal("-1")
    return pnlp

def compute_gain(
        percent_price_gain: Decimal, n_shares: float, start_price: float,
        transactions_value: Optional[Decimal]=Decimal("0")
        ) -> Decimal:
    """Absolute gain with transactions included

    n_shares = number of shares, usually an int, but it could be fractional
    
    percent_gain * n_shares * start_price

    transactions are optional: sum them up outside of this method
    costs are negative
    """
    gain = percent_price_gain*Decimal(str(n_shares))*Decimal(str(start_price))
    
    return gain+transactions_value
    

def compute_percent_gain(
        gain: Decimal, n_shares: float, start_price: float):
    """Compute percent gain including all transactions

    gain: includes transactions

    compute as a percent of initial value of the position
    """
    percent_gain = gain / Decimal(str(n_shares))/Decimal(str(start_price))
    
    return percent_gain

def compute_annualized_percent_gain(
        percent_gain: Decimal, start_date: datetime, end_date: datetime
        ) -> tuple[Decimal, int]:
    """Annualize gain given a percent gain

    It's optional for the percent_gain to include transactions
    """
    days = (end_date - start_date).days
    logger.debug('days: %d', days)

    pct_yr = Decimal(str(days))/Decimal("365.0")
    if (days == 0) or (pct_yr == 0):
        return Decimal('nan'), days
    root_yr = Decimal("1")/pct_yr

    annualized = (percent_gain+Decimal("1"))**root_yr+Decimal("-1")
    return annualized, int(days)


def compute_annualized_gain(gain: Decimal, days: int):
    """Amortized for the year
    """
    pct_yr = Decimal(str(days))/Decimal("365.0")
    if gain >= 0:
        return min(gain/pct_yr, gain)
    
    return max(gain/pct_yr, gain)
