from dagster import AssetExecutionContext
from pandas import DataFrame
from dagster_covid_data.utils.data import ensure_schema_consistency
from dagster_duckdb import DuckDBResource

import duckdb


def append_data_to_duckdb(context: AssetExecutionContext,
                        db_connection: DuckDBResource, 
                        data: DataFrame,
                        schema_name: str, 
                        table_name: str) -> None:
    """
    Appends data to a DuckDB table. If the table does not exist, it is created.
    If the table exists, missing columns are added to the DataFrame and data is inserted.

    Args:
        context (AssetExecutionContext): Dagster execution context for logging.
        db_connection (DuckDBResource): DuckDB resource for database connection.
        data (DataFrame): The data to append.
        schema_name (str): The schema name in DuckDB.
        table_name (str): The table name in DuckDB.

    Returns:
        None
    """
    with db_connection.get_connection() as connection:
        connection.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
        context.log.info(f"{connection}")
    try:
        # Create table name
        with db_connection.get_connection() as connection:
            connection.execute(
                f"""
                    CREATE TABLE {schema_name}.{table_name} AS SELECT * FROM data
                """
            )
    except duckdb.duckdb.CatalogException as e:
        context.log.info(f"Table - {schema_name}.{table_name} already exists.")
        context.log.info(f"Inserting data in {schema_name}.{table_name}.")
        
        required_columns = get_table_columns(db_connection=db_connection,
                                             schema_name=schema_name,
                                             table_name=table_name)
        data = ensure_schema_consistency(data=data, 
                                         required_columns=required_columns) 
        with db_connection.get_connection() as connection:
            connection.execute(
                f"""
                    INSERT INTO {schema_name}.{table_name} SELECT * FROM data
                """
            )

def reload_data_to_duckdb(context: AssetExecutionContext,
                        db_connection: DuckDBResource, 
                        data: DataFrame,
                        schema_name: str, 
                        table_name: str) -> None:
    """
    Replaces all data in a DuckDB table with the provided DataFrame.
    If the table does not exist, it is created.

    Args:
        context (AssetExecutionContext): Dagster execution context for logging.
        db_connection (DuckDBResource): DuckDB resource for database connection.
        data (DataFrame): The data to load.
        schema_name (str): The schema name in DuckDB.
        table_name (str): The table name in DuckDB.

    Returns:
        None
    """
    with db_connection.get_connection() as connection:
        connection.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
        context.log.info(f"{connection}")
    # Create/replace table name
        context.log.info(f"Inserting data in {schema_name}.{table_name}.")
        connection.execute(
            f"""
                CREATE OR REPLACE TABLE {schema_name}.{table_name} AS SELECT * FROM data
            """
        )

def get_table_columns(db_connection: DuckDBResource, schema_name: str, table_name: str) -> list:
    """
    Retrieves the existing columns of a table in DuckDB.

    Args:
        db_connection (DuckDBResource): DuckDB resource for database connection.
        schema_name (str): The name of the schema.
        table_name (str): The name of the table.

    Returns:
        list: A list of existing column names in the specified table. Returns an empty list if the table does not exist.
    """
    try:
        with db_connection.get_connection() as connection:
            query = f"DESCRIBE {schema_name}.{table_name}"
            result = connection.execute(query).fetchall()
    except duckdb.duckdb.CatalogException:
        return []
    else:
        return [row[0] for row in result]