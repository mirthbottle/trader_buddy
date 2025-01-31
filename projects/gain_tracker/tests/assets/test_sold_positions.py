import pytest
import pandas as pd
from datetime import datetime, timedelta, date
from dagster import (
    build_asset_context
)

from gain_tracker.assets.sold_positions import (
    PT_INFO,
    missing_positions
)


TODAY_LOC = datetime.now(tz=PT_INFO).date()
TODAY_LOC_STR = TODAY_LOC.isoformat()
print(TODAY_LOC_STR)


@pytest.fixture
def sample_etrade_positions():
    data = {
        "position_lot_id": [1, 2, 1, 3],
        "date": [
            datetime.now(tz=PT_INFO) - timedelta(days=1),
            datetime.now(tz=PT_INFO) - timedelta(days=2),
            datetime.now(tz=PT_INFO) - timedelta(days=3),
            datetime.now(tz=PT_INFO) - timedelta(days=9),
        ],
        "quantity": [10, 20, 10, 15],
    }
    return pd.DataFrame(data)

@pytest.fixture
def transactions_to_patch():
    return pd.DataFrame()


def test_missing_positions(sample_etrade_positions, transactions_to_patch):

    context = build_asset_context(partition_key=TODAY_LOC_STR)

    result = missing_positions(
        sample_etrade_positions)
    assert not result.empty
    assert len(result) == 3
    assert result.iloc[0]["position_lot_id"] == 1
    assert result.iloc[1]["position_lot_id"] == 2
    assert result.iloc[2]["position_lot_id"] == 3

