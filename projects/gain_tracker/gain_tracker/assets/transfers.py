"""Transfers assets

This module defines the assets related to transfers in the Gain Tracker project.
Pull transfers from E-trade

"""

import pandas as pd
from dagster import (
    asset, Output, AssetExecutionContext,
    AssetIn
)
from . import PT_INFO
from ..partitions import monthly_partdef, daily_to_monthly

@asset(
    partitions_def=monthly_partdef,
    metadata={"partition_expr": "DATETIME(transaction_date)"},
    output_required=False,
    ins={
        "etrade_transactions": AssetIn(
            partition_mapping=daily_to_monthly
        )
    }
)
def transfer_transactions(
    context: AssetExecutionContext,
    etrade_transactions: pd.DataFrame
):
    """Transfer transactions from E-Trade

    This asset processes transfer transactions and returns a DataFrame
    containing the relevant transfer data.
    It takes a daily partitioned DataFrame of E-Trade transactions
    and filters for transactions of type 'Transfer', 'Online Transfer'
    or 'Contribution'
    It then maps to a monthly partitioned DataFrame indexed by account_id_key

    Output:
        pd.DataFrame: A DataFrame containing transfer transactions with the following columns:
            - transaction_id: Unique identifier for the transaction
            - transaction_date: Date of the transaction
            - description: Source of transfer if amount < 0, otherwise destination
            - transaction_type: Type of the transaction (Transfer, Online Transfer, Contribution)
            - amount: Amount of the transaction
            - fee: Fee associated with the transaction
            - account_id: Identifier for the account associated with the transaction
            - timestamp: Timestamp of the transaction
    """
    # Filter for transfer transactions
    transfers = etrade_transactions[
        etrade_transactions['transaction_type'].isin(
            ['Transfer', 'Online Transfer','Contribution'])
    ].copy(deep=True)

    output_cols = [
        "transaction_id", "transaction_date", "transaction_type",
        "description", "amount",
        "fee", "account_id", "timestamp"]    

    if len(transfers) > 0:
        # If no transfers, return an empty DataFrame with the expected columns
        yield Output(transfers[output_cols])


@asset(
    partitions_def=monthly_partdef,
    metadata={"partition_expr": "DATETIME(month)"},
    output_required=False,
    ins={
        "transfer_transactions": AssetIn()
    }
)
def monthly_transfer_totals(
    context: AssetExecutionContext,
    transfer_transactions: pd.DataFrame
):
    """
    Sums the transfer amounts per account for each month.

    Output:
        pd.DataFrame: A DataFrame with columns:
            - account_id
            - total_transfer_amount
            - month
    """        
    grouped = (
        transfer_transactions[["account_id", "amount"]]
        .groupby(["account_id"], as_index=False)
        .sum()
        .rename(columns={"amount": "total_transfer_amount"})
    )
    grouped.loc[:, "month"] = context.partition_key
    grouped.loc[:, "timestamp"] = pd.Timestamp.now(tz=PT_INFO)
    yield Output(grouped.reset_index())