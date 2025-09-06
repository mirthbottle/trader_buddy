"""Portfolio class

A portfolio is made up of many positions

Compute the portfolio gain for an arbitrary number of months
It should take into account transfers in and out of the account

"""
import pandas as pd
from decimal import Decimal, getcontext

getcontext().prec = 12

def percent_portfolio_gain(
        start_value: float, end_value=float, transfer_in=float)->Decimal:
    """Compute the percent gain of a portfolio

    Adjust the ending balance by subtracting the cumulative transfers
    to get the true gain/loss from market movements

    convert to decimals
    """
    pct_gain = (Decimal(str(end_value)) - Decimal(str(transfer_in))) \
        / Decimal(str(start_value)) - Decimal("1.0")
    
    return pct_gain

def compute_portfolio_gains(
        portfolio_balances: pd.DataFrame, monthly_transfer_totals: pd.DataFrame
        ) -> pd.DataFrame:
    """Compute portfolio gain

    For each account Id key and the latest month,
    compute the GainMetrics like for positions
    but take into account transfers in and out of the account
    
    total_account_value
    """
    max_date = portfolio_balances['month'].max()
    current_acct_values = portfolio_balances.set_index(
        ["month", "account_id"]).loc[max_date, "total_account_value"]
    
    balances_transfers = pd.merge(
        portfolio_balances,
        monthly_transfer_totals,
        how='left',
        on=['account_id', 'month']
    ).sort_values(by='month', ascending=False)

    balances_transfers.loc[:, "total_transfer_amount"] = balances_transfers[
        'total_transfer_amount'].fillna(0)
    
    balances_transfers["cumulative_transfers"] = (
        balances_transfers
        .groupby('account_id')['total_transfer_amount'].cumsum()
    )
    # print(balances_transfers)
    
    prev_balances = balances_transfers.loc[
        balances_transfers['month'] < max_date].set_index("account_id")

    prev_balances.loc[:, "current_account_value"] = current_acct_values
    
    prev_balances.loc[:, "percent_gain"] = prev_balances.apply(
        lambda r: percent_portfolio_gain(
            r["total_account_value"], r["current_account_value"], 
            r["cumulative_transfers"]), 
        axis=1
    )
    return prev_balances

class Portfolio:
    """Portfolio class

    load positions from trading_platform
    and update data in db?
    """

    def __init__(self, portfolio_id: str, default_benchmark_ticker:str="IVV"):
        """Initialize a portfolio
        """
        self.portfolio_id = portfolio_id
        self.positions = pd.DataFrame([])
        # self.positions = trading_platform.get_positions_data()
        self.benchmarks = pd.DataFrame([])