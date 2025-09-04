"""Portfolio class

A portfolio is made up of many positions

Compute the portfolio gain for an arbitrary number of months
It should take into account transfers in and out of the account

"""
import pandas as pd

def compute_portfolio_gains(portfolio_balances: pd.DataFrame):
    """Compute portfolio gain

    For each account Id key and the latest month,
    compute the GainMetrics like for positions
    but take into account transfers in and out of the account
    
    totalaccountvalue
    """
    max_date = portfolio_balances['date'].max()
    current_acct_value = portfolio_balances.set_index("date").loc[
        max_date, "totalaccountvalue"]

    prev_balances = portfolio_balances.loc[
        portfolio_balances['date'] < max_date].copy()
    prev_balances.loc[:, "current_account_value"] = current_acct_value
    

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