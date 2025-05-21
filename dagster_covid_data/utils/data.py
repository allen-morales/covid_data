import pandas as pd
import dagster as dg
from datetime import datetime

def missing_date_column_handler(data: pd.DataFrame, context: dg.AssetExecutionContext) -> pd.DataFrame:
    """
        Handles the missing date column in the DataFrame by adding it if it doesn't exist.
    """
    
    date_obj = datetime.strptime(context.partition_key, "%m-%d-%Y")
    new_date_format = date_obj.strftime("%Y-%m-%d")
    if 'date' not in data.columns:
            data['date'] = new_date_format
    return data

def add_ingestion_timestamp(data: pd.DataFrame) -> pd.DataFrame:
    """
    Appends an ingestion timestamp column to the DataFrame.

    Args:
        data (pd.DataFrame): The input DataFrame.

    Returns:
        pd.DataFrame: The DataFrame with an added 'ingestion_timestamp' column.
    """
    ingestion_time = datetime.now()  # Current timestamp as a datetime object
    data['ingestion_timestamp'] = ingestion_time
    return data

def ensure_schema_consistency(data: pd.DataFrame, required_columns: list) -> pd.DataFrame:
    """
    Ensures that the DataFrame has all required columns. Adds missing columns with default values.

    Args:
        data (pd.DataFrame): The input DataFrame.
        required_columns (list): A list of required column names.

    Returns:
        pd.DataFrame: The updated DataFrame with all required columns.
    """
    for column in required_columns:
        if column not in data.columns:
            data[column] = None  # Add missing column with default value
    return data

def rename_columns_to_lowercase(data: pd.DataFrame) -> pd.DataFrame:
    """
    Renames all columns in the DataFrame to lowercase.

    Args:
        data (pd.DataFrame): The input DataFrame.

    Returns:
        pd.DataFrame: The DataFrame with all column names in lowercase.
    """
    data.columns = [col.lower() for col in data.columns]
    return data