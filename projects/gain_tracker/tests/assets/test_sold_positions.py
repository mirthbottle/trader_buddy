import pytest
import pandas as pd
from datetime import datetime, timezone, timedelta, date
from dagster import (
    build_asset_context
)

from gain_tracker.assets.sold_positions import (
    missing_positions, closed_positions
)


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
    
    output = list(result)[0]
    result = output.value
    assert not result.empty
    assert len(result) == 1
    print(result)
    # assert result.loc[1, "quantity_change"] == 0
    assert result.loc[2, "quantity"] == 20
    assert result.loc[2, "price_paid"] == 100
    assert 4 not in result.index

@pytest.fixture
def sample_missing_positions():
    # all sold, all sold, partial sale
    data = {
        "symbol_description": ["AAPL", "GOOGL", "MSFT"],
        "date_acquired": [
            date.fromisoformat("2021-01-01"), 
            date.fromisoformat("2020-05-15"), 
            date.fromisoformat("2019-11-20")],
        "price_paid": [150.0, 2000.0, 120.0],
        "quantity": [100.0, 50.0, 100.0],
        "market_value": [16000.0, 105000.0, 10000.0],
        "original_qty": [100, 50, 300],
        "account_id_key": [101, 102, 103],
        "position_id": [1001, 1002, 1003],
        "position_lot_id": [1, 2, 3],
    }
    yield pd.DataFrame(data)


@pytest.fixture
def sample_sold_transactions():
    data = {
        "symbol": ["GOOGL", "MSFT"],
        "quantity": [50, 100],
        "transaction_type": ["Sold"]*2, 
        "transaction_date": [
            date.fromisoformat("2024-03-26")]*2,
        "fee": [0.01]*2, 
        "transaction_id": [25, 26],
        "amount": [105000, 10000.0] # sold at P=2100, 100
    }
    yield pd.DataFrame(data)


def test_closed_positions(
        sample_sold_transactions, sample_missing_positions):

    result = closed_positions(
        sample_sold_transactions.copy(deep=True),
        sample_missing_positions.copy(deep=True), 
    )
    output = list(result)[0]
    result = output.value
    print(result)
    assert len(result) == 2
    assert result.loc[1, "market_value"] == 10000
    assert result.loc[1, "transaction_fee"] == 0.01
    assert result.loc[1, "timestamp"] == datetime(2024, 3, 26, tzinfo=timezone.utc)
    assert "days_held" in result.columns

    # no_msft_pos = sample_etrade_positions.drop(2)
    # result = closed_positions(
    #     sample_sold_transactions.copy(deep=True),
    #     no_msft_pos,
    # )
    # print(result)
    # assert result is None


@pytest.fixture
def sample_missing_positions_multiple_lot_ids():
    data = {
        "symbol_description": ["AAPL"]*3,
        "date_acquired": [
            date.fromisoformat("2021-01-01"), 
            date.fromisoformat("2020-05-15"), 
            date.fromisoformat("2019-11-20")],
        "price_paid": [150.0, 200.0, 120.0],
        "quantity": [100.0, 50.0, 10.0],
        "market_value": [20000.0, 10000.0, 2000.0], # P=200
        "original_qty": [100, 50, 300],
        "account_id_key": [101, 102, 103],
        "position_id": [1001, 1002, 1003],
        "position_lot_id": [1, 2, 3],
    }
    yield pd.DataFrame(data)

@pytest.fixture
def sample_sold_multiple_lot_ids():
    data = {
        "symbol": ["AAPL"]*3,
        "quantity": [100, 50, 10],
        "transaction_type": ["Sold"]*3, 
        "transaction_date": [
            date.fromisoformat("2024-03-26")]*3,
        "fee": [0.01]*3, 
        "transaction_id": [25, 26, 27],
        "amount": [20000.0, 10000.0, 2000.0] # sold at P=200
    }
    yield pd.DataFrame(data)

def test_closed_position_multiple_position_lot_ids_sold(
        sample_sold_multiple_lot_ids, sample_missing_positions_multiple_lot_ids):
    
    result = closed_positions(
        sample_sold_multiple_lot_ids.copy(deep=True),
        sample_missing_positions_multiple_lot_ids.copy(deep=True), 
    )
    output = list(result)[0]
    result = output.value
    print(result)
    assert len(result) == 3
    assert result.loc[1, "transaction_fee"] == 0.01
    assert "days_held" in result.columns
    assert result.loc[2, "quantity"] == 10
    assert all([s=="AAPL" for s in result["symbol_description"]])

    # case 3: multiple position_lot_ids sold for the same symbol
    # for 1 transaction
    sold_1_transaction = sample_sold_multiple_lot_ids[0:1].copy(deep=True)
    sold_1_transaction.loc[0, "quantity"] = 160
    sold_1_transaction.loc[0, "amount"] = 32000.0

    result = closed_positions(
        sold_1_transaction,
        sample_missing_positions_multiple_lot_ids.copy(deep=True), 
    )
    output = list(result)[0]
    result = output.value
    print(result)
    assert len(result) == 3
    assert result.loc[1, "transaction_fee"] == 0.01*50/160
    assert "days_held" in result.columns
    assert result.loc[2, "quantity"] == 10
    assert all([s=="AAPL" for s in result["symbol_description"]])


@pytest.fixture
def sample_sold_multiples_with_combo():
    data = {
        "symbol": ["AAPL"]*2,
        "quantity": [100, 60],
        "transaction_type": ["Sold"]*2, 
        "transaction_date": [
            date.fromisoformat("2024-03-26")]*2,
        "fee": [0.01]*2, 
        "transaction_id": [25, 26],
        "amount": [20000.0, 12000.0] # sold at P=200
    }
    yield pd.DataFrame(data)

def test_closed_position_multiples_with_combo(
        sample_sold_multiples_with_combo, sample_missing_positions_multiple_lot_ids):
    # 3 position_lot_ids were sold by AAPL, but only 2 transactions
    result = closed_positions(
        sample_sold_multiples_with_combo.copy(deep=True),
        sample_missing_positions_multiple_lot_ids.copy(deep=True), 
    )
    output = list(result)[0]
    result = output.value
    mdata = output.metadata
    print(mdata)
    print(result)
    assert len(result) == 3
    assert mdata["unmatched_count"].value == 0
    # check flags in the metadata


@pytest.fixture
def sample_sold_ambiguous_multiples():
    data = {
        "symbol": ["AAPL"]*2,
        "quantity": [80, 80],
        "transaction_type": ["Sold"]*2, 
        "transaction_date": [
            date.fromisoformat("2024-03-26")]*2,
        "fee": [0.01]*2, 
        "transaction_id": [25, 26],
        "amount": [16000.0, 16000.0] # sold at P=200
    }
    yield pd.DataFrame(data)

def test_closed_position_ambiguous_multiples(
        sample_sold_ambiguous_multiples, sample_missing_positions_multiple_lot_ids):
    # 3 position_lot_ids were sold by AAPL, but only 2 transactions
    result = closed_positions(
        sample_sold_ambiguous_multiples.copy(deep=True),
        sample_missing_positions_multiple_lot_ids.copy(deep=True), 
    )

    output = list(result)[0]

    mdata = output.metadata
    print(mdata)
    print(result)
    assert mdata["unmatched_count"].value == 2
    assert mdata["case_flag"].value == 'U'

@pytest.fixture
def sample_missing_dup_multiples():
    data = {
        "symbol_description": ["AAPL"]*2,
        "date_acquired": [
            date.fromisoformat("2021-01-01"), 
            date.fromisoformat("2019-11-20")],
        "price_paid": [150.0, 120.0],
        "quantity": [80.0, 80.0],
        "market_value": [2000.0, 2000.0], # P=200
        "original_qty": [100, 300],
        "account_id_key": [101, 103],
        "position_id": [1001, 1003],
        "position_lot_id": [1, 3],
    }
    yield pd.DataFrame(data)


def test_closed_position_dup_matches(
        sample_sold_ambiguous_multiples, sample_missing_dup_multiples):
    # 2 position_lot_ids were sold by AAPL, and 2 transactions
    # but we don't know how to match them. 
    # they must be manually matched bc the price_paid and date_acquired will be different
    result = closed_positions(
        sample_sold_ambiguous_multiples.copy(deep=True),
        sample_missing_dup_multiples.copy(deep=True), 
    )
    output = list(result)[0]

    mdata = output.metadata
    print(mdata)
    # assert len(result) == 0
    assert mdata["unmatched_count"].value == 2
    assert mdata["case_flag"].value == 'M'

