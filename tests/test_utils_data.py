import pytest
import pandas as pd
from datetime import datetime
from dagster_covid_data.utils import data

class DummyContext:
    def __init__(self, partition_key):
        self.partition_key = partition_key

def test_missing_date_column_handler_adds_date():
    df = pd.DataFrame({'value': [1, 2]})
    context = DummyContext("06-02-2025")
    result = data.missing_date_column_handler(df.copy(), context)
    assert 'date' in result.columns
    assert all(result['date'] == "2025-06-02")

def test_missing_date_column_handler_keeps_existing_date():
    df = pd.DataFrame({'date': ["2025-06-02"], 'value': [1]})
    context = DummyContext("06-02-2025")
    result = data.missing_date_column_handler(df.copy(), context)
    assert (result['date'] == "2025-06-02").all()

def test_add_ingestion_timestamp():
    df = pd.DataFrame({'a': [1, 2]})
    result = data.add_ingestion_timestamp(df.copy())
    assert 'ingestion_timestamp' in result.columns
    assert pd.api.types.is_datetime64_any_dtype(result['ingestion_timestamp'])

def test_ensure_schema_consistency_adds_missing():
    df = pd.DataFrame({'a': [1, 2]})
    required = ['a', 'b', 'c']
    result = data.ensure_schema_consistency(df.copy(), required)
    assert all(col in result.columns for col in required)
    assert result['b'].isnull().all() and result['c'].isnull().all()

def test_ensure_schema_consistency_no_change_if_all_present():
    df = pd.DataFrame({'a': [1], 'b': [2]})
    required = ['a', 'b']
    result = data.ensure_schema_consistency(df.copy(), required)
    assert list(result.columns) == ['a', 'b']

def test_rename_columns_to_lowercase():
    df = pd.DataFrame({'A': [1], 'B_C': [2]})
    result = data.rename_columns_to_lowercase(df.copy())
    assert list(result.columns) == ['a', 'b_c']