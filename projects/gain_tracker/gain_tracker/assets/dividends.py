"""Handling dividends

It's not possible to know for sure which position_lot_id 
the dividend gets allocated for since there may be some rules
about how long the position has to be held before the dividend 
is allocated. However, we can approximate it by allocating
to any open_positions with the same symbol.

To incorporate dividends into the gain calculation,
we need pull the dividend allocated per position_lot_id
and add it to the gain calculation.

Should this be included in the existing gains asset and
closed_positions asset?

Maybe make downstream assets for ease of implementation
and backwards compatibility. They can be deleted later.

"""
import pandas as pd
from dagster import (
    asset, Output, AssetExecutionContext
)
from ..partitions import daily_partdef, monthly_partdef

def attribute_dividend_to_positions(
        indexed_positions: pd.DataFrame, dividend: pd.Series):
    """attribute to position_lot_ids according to quantity owned

    dividend is one row of dividends
    """
    div_cols = ["transaction_id", "dividend", "dividend_type", "description",
                    "timestamp", "transaction_date"]
    
    # sometimes the dividend transaction_date is on a weekend
    # so there won't be etrade_positions for that date
    # we should select the most recent date before the transaction date then

    positions = indexed_positions.loc[
        dividend['account_id_key'],
        dividend['symbol_description']]

    target_date = max([d for d in positions["date"] if d<=dividend["transaction_date"]])
    print(target_date)
    positions = positions.loc[positions["date"] == target_date].copy(deep=True)
    total_quantity = positions['quantity'].sum()

    dividend_data = dividend[div_cols].to_dict()

    positions = positions.assign(**dividend_data)
    positions.loc[:, "total_quantity"] = total_quantity 
    positions.loc[:, "dividend"] = positions.apply(
        lambda r: r.dividend * r.quantity / total_quantity, axis=1
    )
    return positions

@asset(
    partitions_def=monthly_partdef,
    metadata={"partition_expr": "DATETIME(transaction_date)"},
    output_required=False
)
def position_dividends(
    context: AssetExecutionContext,
    etrade_transactions: pd.DataFrame,
    etrade_positions: pd.DataFrame,
):
    """Dividend transactions from E-Trade
    The dividend transaction date is sometimes a date in the past
    so that's why we can't use daily_partdef

    But we should use the positions for the day of payout.
    That may be slighty different per row of transaction.
    """
    dividends = etrade_transactions.loc[
        etrade_transactions["transaction_type"].str.contains("Dividend")].copy()
    non_eqs = dividends.loc[dividends["security_type"] != "EQ"]
    if len(non_eqs) > 0:
        print(non_eqs.columns)
        print(non_eqs.values)
    dividends = dividends.loc[dividends["security_type"] == "EQ"]

    if len(dividends) > 0:
        dividends = (
            dividends.rename(
                columns={
                    "transaction_type": "dividend_type",
                    "symbol": "symbol_description",
                    "amount": "dividend"})
        )

        # dividends will only have 1 record per account_id_key and symbol
        # we want to pull the positions per 
        # transaction_date, account_id_key, symbol_description
        
        positions_i = etrade_positions.set_index(
            ["account_id_key", "symbol_description"]
        )

        position_divs = pd.concat(
            dividends.apply(
                lambda r: attribute_dividend_to_positions(positions_i, r),
                axis=1).values).reset_index()
        
        output_cols = [
            "symbol_description", "account_id_key", "position_lot_id",
            "transaction_id", "dividend", "description", "dividend_type", 
            "timestamp", # use the timestamp from etrade_transactions 
            "transaction_date",
        ]
        
        yield Output(position_divs[output_cols])