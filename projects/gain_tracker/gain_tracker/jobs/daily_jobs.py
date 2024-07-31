"""Daily jobs
could put it on a schedule

grouped by partition
"""

from dagster import define_asset_job

from ..partitions import daily_partdef, weekly_partdef
from ..assets.positions import *

pull_etrade_job = define_asset_job(
    "pull_etrade_job", 
    selection=[
        etrade_accounts, etrade_transactions, etrade_positions,
        sold_transactions,
        positions_scd4, open_positions,
    ],
    partitions_def=weekly_partdef
)

recommendations_job = define_asset_job(
    "recommendations_job",
    selection=[
        gains, sell_recommendations,
        buy_recommendations_previously_sold,
        all_recommendations 
    ],
    partitions_def=daily_partdef
)