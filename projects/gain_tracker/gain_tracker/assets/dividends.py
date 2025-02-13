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
from ..partitions import daily_partdef

@asset(
    partitions_def=daily_partdef,
    metadata={"partition_expr": "DATETIME(transaction_date)"},
    output_required=False
)
def position_dividends(
    context: AssetExecutionContext,
    etrade_transactions: pd.DataFrame,
    etrade_accounts: pd.DataFrame,
    etrade_positions: pd.DataFrame,
):
    """Dividend transactions from E-Trade"""
    dividends = etrade_transactions.loc[
        etrade_transactions["transaction_type"].str.contains("Dividend")]
    
    if len(dividends) > 0:
        dividends = (
            dividends.set_index("account_id")
            .join(etrade_accounts.set_index("account_id")[["account_id_key"]])
            .rename(
                columns={
                    "transaction_type": "dividend_type",
                    "symbol": "symbol_description",
                    "amount": "dividend"})
            .reset_index()
            .set_index(["account_id_key", "symbol_description"])
        )

        # dividends will only have 1 record per account_id_key and symbol
        # we want an inner join
        # verify that position_lot_id is not duplicated
        div_cols = ["transaction_id", "dividend", "dividend_type", "description",
                    "timestamp", "total_quantity"]
        
        position_sums = etrade_positions.groupby(
            ["account_id_key", "symbol_description"]
        )[["quantity"]].sum().rename({"quantity": "total_quantity"})
        
        dividends = dividends.join(position_sums, how="inner")

        position_divs = etrade_positions.set_index(
            ["account_id_key", "symbol_description"]).join(
                dividends[div_cols], how="inner")
        
        position_divs.loc[:, "dividends"] = position_divs.apply(
            lambda r: r.dividend * r.quantity / r.total_quantity, axis=1
        )

        output_cols = [
            "symbol_description", "date", "account_id_key", "position_lot_id",
            "transaction_id", "dividend", "description", "dividend_type", 
            "timestamp", # use the timestamp from etrade_transactions 
        ]
        
        yield Output(position_divs[output_cols])