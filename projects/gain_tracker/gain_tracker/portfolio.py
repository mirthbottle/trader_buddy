"""Portfolio class

A portfolio is made up of many positions

Compute the portfolio gain for an arbitrary number of months
It should take into account transfers in and out of the account

"""
import pandas as pd


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