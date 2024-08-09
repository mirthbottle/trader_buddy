"""Daily jobs
could put it on a schedule

grouped by partition
"""

from dagster import define_asset_job

from ..partitions import daily_partdef, weekly_partdef
from ..assets.positions import *

pull_etrade_dailies = define_asset_job(
    "pull_etrade_dailies",
    selection=[
        etrade_accounts, etrade_positions
    ]
)

pull_etrade_weeklies = define_asset_job(
    "pull_etrade_weeklies", 
    selection=[
        etrade_transactions,
        sold_transactions,
        open_positions_window,
        closed_positions
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