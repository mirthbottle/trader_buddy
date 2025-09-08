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
from dagster import asset, AssetExecutionContext, Output, AssetIn, TimeWindowPartitionMapping

from ..partitions import monthly_partdef
from ..resources.etrade_resource import ETrader, camel_to_snake
from ..portfolio import compute_portfolio_gains

def get_account_balances(keys: list[str], etrader: ETrader) -> pd.DataFrame:
    """Get balances for a list of account keys"""
    balances = []
    for key in keys:
        balance = etrader.get_account_balance(key)
        if balance is not None:
            balance['accountIdKey'] = key
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
            "partition_expr": "DATETIME(month)"},
)
def portfolio_balances(
    context: AssetExecutionContext, etrader: ETrader, etrade_accounts: pd.DataFrame) -> Output:
    """
    Dagster asset to pull account balances using the ETrader resource.
    """
    partition_date_str = context.partition_key
    partition_date = date.fromisoformat(partition_date_str)

    keys = etrade_accounts['account_id_key'].unique().tolist()

    balances = get_account_balances(keys, etrader)
    balances.loc[:, 'month'] = partition_date
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
        "partition_expr": "DATETIME(month)"
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
    context: AssetExecutionContext,
    portfolio_balances: pd.DataFrame, monthly_transfer_totals: pd.DataFrame
) -> Output[pd.DataFrame]:
    """Compute portfolio performance from account balances

    For each account Id key and the latest month,
    compute the GainMetrics like for positions
    but take into account transfers in and out of the account

    gains will be in the column 'percent_gain'
    
    """
    if portfolio_balances.empty:
        return Output(pd.DataFrame([]))

    portfolio_gains = compute_portfolio_gains(
        portfolio_balances, monthly_transfer_totals)
    
    # Compute the difference in months between each row's 'month' and the partition_key month
    partition_month = pd.to_datetime(context.partition_key)

    portfolio_gains['months_ago'] = (
        partition_month.year - pd.to_datetime(portfolio_gains['month']).dt.year
    ) * 12 + (
        partition_month.month - pd.to_datetime(portfolio_gains['month']).dt.month
    )
    
    # Reindex columns, filling missing with NaN
    # final_cols = ['account_id'] + list(range(1, 12))
    rename_month_cols = {i: f"{i+1}m_percent_gain" for i in range(0, 12)}

    portfolio_gains_months_ago = portfolio_gains.pivot_table(
        index='account_id',
        columns='months_ago',
        values='percent_gain'
    )
    
    formatted = (
        portfolio_gains_months_ago.reindex(columns=list(range(0, 12)))
        .rename(columns=rename_month_cols).reset_index()
    )

    # Pivot cumulative_transfers by account_id and months_ago
    transfers_pivot = portfolio_gains.pivot_table(
        index='account_id',
        columns='months_ago',
        values='cumulative_transfers'
    )
    rename_transfer_cols = {i: f"{i+1}m_transfers" for i in range(0, 12)}
    transfers_formatted = (
        transfers_pivot.reindex(columns=list(range(0, 12)))
        .rename(columns=rename_transfer_cols).reset_index()
    )

    # Merge formatted and transfers_formatted on account_id
    formatted = pd.merge(formatted, transfers_formatted, on='account_id', how='left')

    return Output(formatted)