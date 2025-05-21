from settings import covid_data_source_url, covid_raw_schema_name
from dagster_covid_data.utils.duckdb import reload_data_to_duckdb
from dagster_covid_data.utils.data import rename_columns_to_lowercase, add_ingestion_timestamp
from dagster_duckdb import DuckDBResource

from pathlib import Path

import dagster as dg
import pandas as pd


daily_partition = dg.DailyPartitionsDefinition(start_date="01-01-2021",
                                               end_date="03-10-2023",
                                                 fmt="%m-%d-%Y")

def ingest_data(context, duckdb: DuckDBResource) -> None:
    
    asset_name = context.asset_key.to_user_string()
    report_url = f"{covid_data_source_url}/{Path(__file__).stem}/{asset_name}.csv"

    context.log.info(f"Ingesting Data - {asset_name}.")
    context.log.info(f"Report URL: {report_url}.")

    try:
        data = pd.read_csv(report_url)
    except Exception as e:
        context.log.error(f"Error: Unable to fetch or parse the CSV file. Details: {e}")
        context.add_output_metadata({"num_rows": 0})
    else:
        context.log.info(f"Data for {asset_name} fetched successfully.")
        
        data = rename_columns_to_lowercase(data)
        data = add_ingestion_timestamp(data)
    
        reload_data_to_duckdb(context=context,
                              duckdb=duckdb,
                              data=data,
                              schema_name=covid_raw_schema_name,
                              table_name=asset_name)
        
        # Log some metadata about the new table. It will show up in the UI.
        context.add_output_metadata({"num_rows": data.shape[0]})

@dg.asset(
    group_name="time_series_ingestion",
    kinds={"python", "duckdb"}
)
def time_series_covid19_confirmed_US(context: dg.AssetExecutionContext, duckdb: DuckDBResource) -> None:
    ingest_data(context, duckdb)

@dg.asset(
    group_name="time_series_ingestion",
    kinds={"python", "duckdb"}
)
def time_series_covid19_confirmed_global(context: dg.AssetExecutionContext, duckdb: DuckDBResource) -> None:
    ingest_data(context, duckdb)

@dg.asset(
    group_name="time_series_ingestion",
    kinds={"python", "duckdb"}
)
def time_series_covid19_deaths_US(context: dg.AssetExecutionContext, duckdb: DuckDBResource) -> None:
    ingest_data(context, duckdb)

@dg.asset(
    group_name="git_hub_time_series_ingestioningestion",
    kinds={"python", "duckdb"}
)
def time_series_covid19_deaths_global(context: dg.AssetExecutionContext, duckdb: DuckDBResource) -> None:
    ingest_data(context, duckdb)

@dg.asset(
    group_name="time_series_ingestion",
    kinds={"python", "duckdb"}
)
def time_series_covid19_recovered_global(context: dg.AssetExecutionContext, duckdb: DuckDBResource) -> None:
    ingest_data(context, duckdb)