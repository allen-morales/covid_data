import pandas as pd
from dagster import asset, AssetExecutionContext, Config
import duckdb

from settings import DUCK_DATABASE_PATH


class Top5CommonValuesConfig(Config):
    schema_name: str
    table_name: str

@asset(
    group_name="analysis_assets",
    kinds={"python", "duckdb"}
)
def top_5_common_values_by_table(context: AssetExecutionContext, config: Top5CommonValuesConfig) -> dict:
    """
    Returns the top 5 most common values for all columns in the given table.
    
    Args:
        context (AssetExecutionContext): Dagster execution context.
        table_name (str): The name of the table to analyze.

    Returns:
        dict: A dictionary where keys are column names and values are DataFrames of the top 5 common values.
    """
    # Connect to DuckDB
    connection = duckdb.connect(DUCK_DATABASE_PATH)

    # Load the table into a Pandas DataFrame
    query = f"SELECT * FROM {config.schema_name}.{config.table_name}"
    df = pd.read_sql(query, connection)

    # Dictionary to store top 5 values for each column
    top_values = {}

    # Iterate over all columns in the DataFrame
    for column in df.columns:
        # Get the top 5 most common values for the column
        top_values[column] = (
            df[column]
            .value_counts()
            .head(5)
            .reset_index()
            .rename(columns={"index": column, column: "count"})
        )
        # Log the result
        context.log.info(f"Top 5 common values for column '{column}':\n{top_values[column]}")

    # Close the DuckDB connection
    connection.close()

    return top_values