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
    missing_prev_pos.loc[:, "date"] = today_loc

    return Output(
        missing_prev_pos, 
        metadata={"prev_date": existing_parts[1].isoformat()})

@asset(
    partitions_def=daily_partdef,
    metadata={"partition_expr": "DATETIME(date_closed)"},
    output_required=False
)
def closed_positions(
    sold_transactions: pd.DataFrame, missing_positions: pd.DataFrame
):
    """
    replaces the old closed_positions

    missing_positions is a dataframe where the quantity is positive
    for much is missing (sold) from the current positions

    sold_transactions is a dataframe where quantity is also positive

    edge cases handled:
    1. position_lot_id sold completely
    2. position_lot_id sold partially (indistinguishable from above)
    3. more than 1 position_lot_id sold for the same symbol
      handle by seeing if there's a match after summing the quantities
      by symbol.

    edge case not handled but will be flagged:
    We need to flag these siutations and handle them with transitions_to_patch
    4. 2 or more sale transactions cover the total quantity of positions sold
      per symbol but they both sold some of the same position_lot_id
    5. (case flag M) multiple sale transactions where each transaction has the same quantity
      The merge will result in duplicates bc of combos. 
    """
    case_flag = ""
    case_message = ""
    unmatched_count = 0

    closing_cols = [
        "timestamp", "date_closed", "transaction_fee", "transaction_id"]
    positions_cols = [
        "account_id_key", "position_id", "price_paid",
        "date_acquired", "original_qty", "position_lot_id"]
    
    missing1 = missing_positions.set_index(["symbol_description", "quantity"])
    closed_transactions = (
        sold_transactions
        .rename(columns={
            "symbol": "symbol_description",
            "fee": "transaction_fee", "amount": "market_value",
            "transaction_date": "date_closed"})
        .set_index([
            "symbol_description", "quantity"])
    )
    closed_transactions.loc[:, "timestamp"] = closed_transactions[
        "date_closed"].apply(
            lambda d: datetime.combine(d, datetime.min.timetz(), tzinfo=timezone.utc))
    
    # inner join, deal with edge cases later
    new_closed_positions = (
        pd.merge(
            missing1[positions_cols],
            closed_transactions[closing_cols+["market_value"]],
            left_index=True, right_index=True, how="inner",
            suffixes=("_pos", None)) # add suffix to missing_positions cols
        .reset_index()
    )
    # get duplicates to detect case 5
    dup_matches = new_closed_positions.loc[
        new_closed_positions.duplicated(subset=["position_lot_id"], keep="first")]
    
    # print(f"dup matches:\n{dup_matches}")
    if len(dup_matches) > 0:
        # remove from new_closed_positions for manual matching
        case_flag = 'M'
        unmatched_count = len(dup_matches)
        case_message = "multiple sales and positions with the same quantity"
        new_closed_positions = new_closed_positions.loc[
            ~new_closed_positions['position_lot_id'].isin(dup_matches['position_lot_id'])]

    # case 3: multiple position_lot_ids sold for the same symbol
    # and for 1 transaction
    # Is it possible for the sum of quantities to match 2 transactions?
    # 4 positions get sold over 2 transactions, no it won't match either transaction
    # bc the sum of positions will be greater than either transaction
    missing2 = missing1.loc[
        ~missing1["position_lot_id"].isin(
            new_closed_positions["position_lot_id"])]
    if len(missing2) > 0:
        missing2_g = missing2.reset_index().groupby("symbol_description")
        missing2_agg = missing2_g.apply(
            lambda g: pd.Series(
                {"quantity": g["quantity"].sum(), 
                 "position_lot_ids": g["position_lot_id"].tolist()}))
        missing2_agg = missing2_agg.reset_index().set_index(
            ["symbol_description", "quantity"])
        # print(f"missing2:\n{missing2_agg}")

        closed2 = closed_transactions.loc[
            ~closed_transactions["transaction_id"].isin(
                new_closed_positions["transaction_id"])]
        new_closed2 = (
            pd.merge(
                missing2_agg,
                closed2[closing_cols+["market_value"]],
                left_index=True, right_index=True, how="inner",
                suffixes=("_pos", None)) # add suffix to missing_positions cols
            .reset_index()  
        )
        if len(new_closed2) > 0:
            # need to expand out the position_lot_ids in missing2_agg["position_lot_ids"]
            new_closed2_expanded = (
                new_closed2.explode("position_lot_ids")
                .rename(columns={"position_lot_ids": "position_lot_id"})
            )
            expanded_rows = missing1.loc[missing1["position_lot_id"].isin(
                new_closed2_expanded["position_lot_id"])]
            new_closed2_final = pd.merge(
                new_closed2_expanded,
                expanded_rows[positions_cols].reset_index(),
                on="position_lot_id", suffixes=("", "_pos"))
            # need to adjust transaction_fee and market_value by quantity_pos/quantity
            new_closed2_final.loc[:, "transaction_fee"] = new_closed2_final.apply(
                lambda r: r["transaction_fee"] * r["quantity_pos"]/r["quantity"], axis=1)
            new_closed2_final.loc[:, "market_value"] = new_closed2_final.apply(
                lambda r: r["market_value"] * r["quantity_pos"]/r["quantity"], axis=1)
            new_closed2_final = (
                new_closed2_final.rename(columns={"quantity": "quantity_transaction"})
                .rename(columns={"quantity_pos": "quantity"})
            )
            print(f"new_closed2_final:\n{new_closed2_final}")
            dup_matches2 = new_closed2_final.loc[
                new_closed2_final.duplicated(subset=["position_lot_id"], keep="first")]
            if len(dup_matches2) > 0:
                # something impossible happened
                raise Exception("duplicates found after matching on quantity grouped by symbol")
            new_closed_positions = pd.concat([new_closed_positions, new_closed2_final])
            # print(new_closed_positions[[
            #     "quantity", "quantity_transaction", "original_qty", "price_paid"]])

    # if there are still unmatched transactions then we have case 4
    closed3 = closed_transactions.loc[
        ~closed_transactions["transaction_id"].isin(
            new_closed_positions["transaction_id"])]
    if len(closed3) > unmatched_count:
        case_flag += 'U'
        case_message = "unmatched transactions left over"
        unmatched_count = len(closed3)
        # there could still be other matches

    if len(new_closed_positions) > 0:
        positions = new_closed_positions.apply(
            lambda r: ClosedPosition(
                r["position_lot_id"],
                r["account_id_key"],
                r["symbol_description"],
                r["price_paid"],
                r["date_acquired"],
                r["quantity"],
                r["original_qty"],
                r["market_value"],
                transaction_id = r["transaction_id"],
                transaction_fee = r["transaction_fee"],
                date_closed=r["date_closed"]), axis=1)
        
        gmetrics = positions.apply(lambda p: p.compute_gains())

        gm_df = pd.DataFrame(gmetrics.values.tolist())

        closed_gains_df = pd.concat([
            new_closed_positions.reset_index(drop=True),gm_df],axis=1)
        
        cols = [
        "symbol_description", "date_closed", "date_acquired", "price_paid", "quantity",
        "market_value", "original_qty", "account_id_key", "position_id", "position_lot_id",
        "timestamp", "transaction_id", "transaction_fee",
        "percent_price_gain", "gain", "percent_gain",
        "annualized_pct_gain", "days_held"]
        return Output(
            closed_gains_df[cols],
            metadata={
                'case_flag': case_flag, 'case_message': case_message, 
                'unmatched_count': unmatched_count}
        )
    return Output(
        None, 
        metadata={
            'case_flag': case_flag, 'case_message': case_message, 
            'unmatched_count': unmatched_count})