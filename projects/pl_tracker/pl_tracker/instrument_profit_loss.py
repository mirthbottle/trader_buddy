"""Compute unrealized gains and losses

"""

from datetime import datetime
from decimal import Decimal, getcontext
getcontext().prec = 12

def profit_loss(
        pnl_pct: Decimal, n_shares: float, start_price: float
        ) -> Decimal:
    """Absolute profit loss

    n_shares = number of shares, usually an int, but it could be fractional
    
    profit loss percent * n_shares * start_price
    """
    pnl = pnl_pct*Decimal(str(n_shares))*Decimal(str(start_price))
    
    return pnl
    
def profit_loss_amortized_year(pnl: Decimal, days: int):
    """Amortized for the year
    """
    pct_yr = Decimal(str(days))/Decimal("365.0")
    if pnl >= 0:
        return min(pnl/pct_yr, pnl)
    else:
        return max(pnl/pct_yr, pnl)

def profit_loss_annualized_percent(
        pnl_pct: Decimal, start_date: datetime, end_date: datetime
        ) -> tuple[Decimal, int]:
    """Annualize profit loss
    """
    days = (end_date - start_date).days
    pct_yr = Decimal(str(days))/Decimal("365.0")
    root_yr = Decimal("1")/pct_yr

    annualized = (pnl_pct+Decimal("1"))**root_yr+Decimal("-1")
    return annualized, days

def profit_loss_percent(
        start_price: float, end_price: float
        ) -> Decimal:
    """Compute profit loss

    scale -1 to 1
    eg. 0.1 = 10%
    """
    pnlp = Decimal(str(end_price))/Decimal(str(start_price))+Decimal("-1")
    return pnlp