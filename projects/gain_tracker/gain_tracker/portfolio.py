"""Portfolio class

A portfolio is made up of many positions
"""
import pandas as pd
from .position import Position

def convert_etrade_to_position_obj(etrade_position_dict:dict):
    """Convert to position object
    
    """
    p = Position(
        etrade_position_dict["symbolDescription"], 
        "USA", "ticker",
        position_entry_quote=etrade_position_dict["pricePaid"],
        position_entry_date=etrade_position_dict["dateAcquired"].,
        position_size=etrade_position_dict["quantity"])

def get_and_save_accounts(trading_platform, db_connection):
    """Get list of accounts
    not sure where this lives yet, possibly in a DAG?
    """
    accounts = trading_platform.list_accounts()

    for account in accounts:
        acc_id_key = account["accountIdKey"]
        p = Portfolio(acc_id_key)
        acc_positions = pd.DataFrame(trading_platform.view_portfolio(acc_id_key))
        acc_positions.loc[:, "position_obj"] = acc_positions.apply(convert_etrade_to_position_obj)

        # not sure if it's efficient to use objects or DataFrames
        # will start writing the dagster assets
    return accounts

def load_accounts(db):
    """Load from db

    This can load old closed accounts and positions, too

    just assume I'm the only user
    """
    accounts = db.get_accounts()

    for account in accounts:
        portfolio_id = account["portfolio_id"]
        p = Portfolio(portfolio_id)
        acc_positions = pd.DataFrame(db.get_positions(portfolio_id))
        acc_positions.loc[:, "position_obj"] = acc_positions.apply(convert_etrade_to_position_obj)
        
    return accounts

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