"""project partitions

may be used by assets or jobs
"""

from dagster import (
    WeeklyPartitionsDefinition, DailyPartitionsDefinition,
    MonthlyPartitionsDefinition, TimeWindowPartitionMapping
)

# this could be configurable by environment
PARTITIONS_START_DATE="2024-02-04"

daily_partdef = DailyPartitionsDefinition(
    start_date=PARTITIONS_START_DATE, end_offset=1)
daily_week_partdef = DailyPartitionsDefinition(
    start_date=PARTITIONS_START_DATE, end_offset=1)
weekly_partdef = WeeklyPartitionsDefinition(
    start_date=PARTITIONS_START_DATE, end_offset=1)
monthly_partdef = MonthlyPartitionsDefinition(
    start_date=PARTITIONS_START_DATE, end_offset=1)

daily_to_monthly = TimeWindowPartitionMapping(
    allow_nonexistent_upstream_partitions=True
)
