"""Daily jobs
could put it on a schedule

grouped by partition
"""

from dagster import define_asset_job

from ..partitions import daily_partdef, weekly_partdef
from ..assets.positions import (
    etrade_accounts, etrade_positions, etrade_transactions,
    sold_transactions,
    gains, sell_recommendations,
    buy_recommendations_previously_sold, all_recommendations,
    benchmark_values)
from ..assets.dividends import position_dividends
from gain_tracker.assets.sold_positions import (
    missing_positions, closed_positions)

pull_etrade_dailies = define_asset_job(
    "pull_etrade_dailies",
    selection=[
        etrade_accounts, etrade_positions,
        etrade_transactions, sold_transactions,
        missing_positions, closed_positions,
        gains, sell_recommendations,
        buy_recommendations_previously_sold,
        all_recommendations 
    ]
)
