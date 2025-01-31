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
    asset, AssetExecutionContext, AssetIn, TimeWindowPartitionMapping,
    # multi_asset, AssetOut, AssetKey,
    Output, Config,
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
def missing_positions(
    context: AssetExecutionContext, etrade_positions: pd.DataFrame):
    """
    What's missing from today's open positions compared to
    the last day of the previous week

    Compute with daily position diffs
    - for each position_lot_id, new quantity - old quantity
    """
    partition_date_str = context.partition_key
    today_loc = date.fromisoformat(partition_date_str)
    
    existing_parts = sorted(
        etrade_positions["date"].unique(), reverse=True)
    
    positions_datei = etrade_positions.set_index(["date", "position_lot_id"])
    
    cols = ["quantity"]
    current_positions = positions_datei.loc[existing_parts[0]][cols]
    prev_positions = positions_datei.loc[existing_parts[1]][cols]
    
    changes = pd.merge(
        prev_positions, current_positions, 
        left_index=True, right_index=True,
        how="outer", suffixes=("_prev", None)).fillna(0)
    changes.loc[:, "quantity_sold"] = changes['quantity_prev'] - changes['quantity']
    
    missing_lot_ids = changes.loc[changes['quantity_sold'] > 0]
    
    missing_prev_pos = positions_datei.loc[existing_parts[1]].loc[
        missing_lot_ids.index]
    
    # replace prev amount with the quantity changed
    missing_prev_pos.loc[:, "quantity"] = missing_lot_ids["quantity_sold"]

    return Output(
        missing_prev_pos, 
        metadata={"prev_date": existing_parts[1].isoformat()})

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