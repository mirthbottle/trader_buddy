"""Transfers assets

This module defines the assets related to transfers in the Gain Tracker project.
Pull transfers from E-trade

"""

import pandas as pd
from dagster import (
    asset, Output, AssetExecutionContext,
    AssetIn
)
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
def transfer_transactions(etrade_transactions: pd.DataFrame) -> pd.DataFrame:
    """Transfer transactions from E-Trade

    This asset processes transfer transactions and returns a DataFrame
    containing the relevant transfer data.
    It takes a daily partitioned DataFrame of E-Trade transactions
    and filters for transactions of type 'Transfer', 'Online Transfer'
    or 'Contribution'
    It then maps to a monthly partitioned DataFrame indexed by account_id_key
    """
    # Filter for transfer transactions
    transfers = etrade_transactions[
        etrade_transactions['transaction_type'] == 'TRANSFER'
    ].copy(deep=True)

    # Ensure the DataFrame is indexed by account_id_key and symbol_description
    transfers.set_index(['account_id_key'], inplace=True)

    # Return the processed transfers DataFrame
    return transfers     