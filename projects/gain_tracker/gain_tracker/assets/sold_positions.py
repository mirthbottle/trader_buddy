"""
replace open_positions_window to something with a weekly partition
diff one pertition with the prev partition
"""

import logging
from typing import Optional
from datetime import date, datetime, timezone, timedelta
from zoneinfo import ZoneInfo

import pandas as pd
import pyarrow as pa

from google.api_core.exceptions import NotFound
from dagster import (
    asset, AssetIn, TimeWindowPartitionMapping,
    # multi_asset, Output, AssetOut, AssetKey,
    AssetExecutionContext, Config,
    )

logger = logging.getLogger(__name__)

from ..partitions import daily_partdef, weekly_partdef

from .. import position_gain as pg
from ..position import ClosedPosition, OpenPosition

PT_INFO = ZoneInfo("America/Los_Angeles")

@asset
def transactions_to_patch():
    """Load from google sheet
    """
    pass

last_7days_partition = TimeWindowPartitionMapping(
    start_offset=-7, end_offset=0, 
    allow_nonexistent_upstream_partitions=True
)

@asset(
        partitions_def=daily_partdef,
        metadata={"partition_expr": "DATETIME(date)"},
        ins={
            "etrade_positions": AssetIn(
                partition_mapping=last_7days_partition
            )
        }
)
def missing_positions(etrade_positions: pd.DataFrame):
    """
    What's missing from today's open positions compared to
    the last day of the previous week

    Compute with daily position diffs
    - for each position_lot_id, new quantity - old quantity
    """
    today_loc = datetime.now(tz=PT_INFO).date()
    start_date = (today_loc) - timedelta(days=7)
    print(start_date)

    positions = (
        etrade_positions
        .sort_values(by="date", ascending=False)    
        .drop_duplicates(
            subset=["position_lot_id", "quantity"], keep="first")
        )   
    return positions

@asset(
    partitions_def=weekly_partdef,
    metadata={"partition_expr": "DATETIME(date_closed)"},
    output_required=False
)
def closed_positions(
    sold_transactions: pd.DataFrame, missing_positions: pd.DataFrame
):
    """
    replaces the old closed_positions
    """
    pass