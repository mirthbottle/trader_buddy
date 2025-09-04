"""Compute performance metrics for a portfolio of assets.

By default compute monthly for the past 12 months
Save as a list?

Take transfers into account

Do dividends and fees need to be taken into account?
They are already included by default in the account balances

For computations, use the position_gain module
"""

from datetime import date, datetime, timezone
import pandas as pd
from dagster import asset, Output, AssetIn, TimeWindowPartitionMapping

from ..partitions import monthly_partdef
from ..resources.etrade_resource import ETrader, camel_to_snake

def get_account_balances(keys: list[str], etrader: ETrader) -> pd.DataFrame:
    """Get balances for a list of account keys"""
    balances = []
    for key in keys:
        balance = etrader.get_account_balance(key)
        if balance is not None:
            balance['account_id_key'] = key
            balances.append(balance)
    if len(balances) == 0:
        return pd.DataFrame([])
    balances_df =  pd.DataFrame(balances)
    snake_cols = {c:camel_to_snake(c) for c in balances_df.columns}
    balances_df.rename(columns=snake_cols, inplace=True)
    return balances_df

@asset(
        partitions_def=monthly_partdef,
        metadata={
            "partition_expr": "DATETIME(transaction_date)"},
)
def portfolio_balances(context, etrader: ETrader, etrade_accounts: pd.DataFrame) -> Output:
    """
    Dagster asset to pull account balances using the ETrader resource.
    """
    partition_date_str = context.partition_key
    partition_date = date.fromisoformat(partition_date_str)

    keys = etrade_accounts['account_id_key'].unique().tolist()

    balances = get_account_balances(keys, etrader)
    balances.loc[:, 'date'] = partition_date
    balances.loc[:, "timestamp"] = datetime.now(timezone.utc)

    return Output(balances)

last_12months_partition = TimeWindowPartitionMapping(
    start_offset=-11,  # include past 12 months (current + 11 previous)
    end_offset=0,
    allow_nonexistent_upstream_partitions=True
)

@asset(
    partitions_def=monthly_partdef,
    metadata={
        "partition_expr": "DATETIME(date)"
    },
    ins={
        "portfolio_balances": AssetIn(
            partition_mapping=last_12months_partition
        ),
        "monthly_transfer_totals": AssetIn(
            partition_mapping=last_12months_partition
        )
    }
)
def portfolio_gains(
    portfolio_balances: pd.DataFrame, monthly_transfer_totals: pd.DataFrame
) -> pd.DataFrame:
    """Compute portfolio performance from account balances

    For each account Id key and the latest month,
    compute the GainMetrics like for positions
    but take into account transfers in and out of the account

    For now, just return the balances
    """
    if portfolio_balances.empty:
        return pd.DataFrame([])

    acct_groups = portfolio_balances.groupby('account_id_key')

    
    # Convert date column to datetime
    portfolio_balances['date'] = pd.to_datetime(portfolio_balances['date'])

    # Sort by date
    portfolio_balances = portfolio_balances.sort_values(by='date')

    return portfolio_balances