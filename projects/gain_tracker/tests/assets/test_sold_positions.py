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
        "position_lot_id": [1, 1, 2, 2, 4, 5, 6],
        "date": [
            date.fromisoformat("2025-01-30"),
            date.fromisoformat("2025-01-30") - timedelta(days=2), # being held
            date.fromisoformat("2025-01-30") - timedelta(days=2),
            date.fromisoformat("2025-01-30") - timedelta(days=3),
            date.fromisoformat("2025-01-30") - timedelta(days=5), # sold 4 days ago
            date.fromisoformat("2025-01-30"), # just bought today
            date.fromisoformat("2025-01-30") # just bought today
        ],
        "quantity": [10, 10, 20, 15, 4, 4, 4],
        "price_paid": [100]*7
    }
    return pd.DataFrame(data)

@pytest.fixture
def transactions_to_patch():
    return pd.DataFrame()


def test_missing_positions(sample_etrade_positions, transactions_to_patch):

    context = build_asset_context(partition_key="2025-01-30")

    result = missing_positions(
        context, sample_etrade_positions)
    result = result.value
    assert not result.empty
    assert len(result) == 1
    print(result)
    # assert result.loc[1, "quantity_change"] == 0
    assert result.loc[2, "quantity"] == 20
    assert result.loc[2, "price_paid"] == 100
    assert 4 not in result.index

