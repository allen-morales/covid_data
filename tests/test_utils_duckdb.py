import pytest
import pandas as pd
from unittest.mock import MagicMock, patch

from dagster_covid_data.utils import duckdb as duckdb_utils

@pytest.fixture
def mock_context():
    ctx = MagicMock()
    ctx.log.info = MagicMock()
    return ctx

@pytest.fixture
def mock_db_resource():
    mock_conn = MagicMock()
    mock_conn.execute = MagicMock()
    resource = MagicMock()
    resource.get_connection.return_value.__enter__.return_value = mock_conn
    return resource

def test_get_table_columns_returns_columns(mock_db_resource):
    mock_conn = mock_db_resource.get_connection.return_value.__enter__.return_value
    mock_conn.execute.return_value.fetchall.return_value = [("col1",), ("col2",)]
    cols = duckdb_utils.get_table_columns(mock_db_resource, "myschema", "mytable")
    assert cols == ["col1", "col2"]

def test_get_table_columns_catalog_exception(mock_db_resource):
    mock_conn = mock_db_resource.get_connection.return_value.__enter__.return_value
    mock_conn.execute.side_effect = duckdb_utils.duckdb.duckdb.CatalogException("error")
    cols = duckdb_utils.get_table_columns(mock_db_resource, "myschema", "mytable")
    assert cols == []

def test_reload_data_to_duckdb_runs_commands(mock_context, mock_db_resource):
    df = pd.DataFrame({"a": [1, 2]})
    duckdb_utils.reload_data_to_duckdb(mock_context, mock_db_resource, df, "myschema", "mytable")
    mock_conn = mock_db_resource.get_connection.return_value.__enter__.return_value
    assert mock_conn.execute.call_count == 2  # CREATE SCHEMA and CREATE OR REPLACE TABLE

@patch("dagster_covid_data.utils.duckdb.get_table_columns", return_value=["a"])
@patch("dagster_covid_data.utils.duckdb.ensure_schema_consistency", side_effect=lambda data, required_columns: data)
def test_append_data_to_duckdb_existing_table(mock_ensure, mock_get_cols, mock_context, mock_db_resource):
    df = pd.DataFrame({"a": [1, 2]})
    # Simulate CatalogException on CREATE TABLE
    mock_conn = mock_db_resource.get_connection.return_value.__enter__.return_value
    def execute_side_effect(sql):
        if "CREATE TABLE" in sql:
            raise duckdb_utils.duckdb.duckdb.CatalogException("exists")
        return MagicMock()
    mock_conn.execute.side_effect = execute_side_effect

    duckdb_utils.append_data_to_duckdb(mock_context, mock_db_resource, df, "myschema", "mytable")
    # Should call ensure_schema_consistency and INSERT INTO
    assert mock_ensure.called
    assert mock_get_cols.called

def test_append_data_to_duckdb_creates_table(mock_context, mock_db_resource):
    df = pd.DataFrame({"a": [1, 2]})
    # No exception on CREATE TABLE
    mock_conn = mock_db_resource.get_connection.return_value.__enter__.return_value
    mock_conn.execute.side_effect = [None, None]  # CREATE SCHEMA, CREATE TABLE
    duckdb_utils.append_data_to_duckdb(mock_context, mock_db_resource, df, "myschema", "mytable")
    assert mock_conn.execute.call_count == 2