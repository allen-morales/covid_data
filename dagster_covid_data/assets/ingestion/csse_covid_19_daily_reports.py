from settings import covid_data_source_url, covid_raw_schema_name
from dagster_covid_data.utils.duckdb import append_data_to_duckdb
from dagster_covid_data.utils.data import missing_date_column_handler, rename_columns_to_lowercase, add_ingestion_timestamp
from dagster_duckdb import DuckDBResource

import dagster as dg
import pandas as pd


daily_partition = dg.DailyPartitionsDefinition(start_date="01-01-2021",
                                               end_date="03-10-2023",
                                                 fmt="%m-%d-%Y")

def ingest_data(context, duckdb: DuckDBResource) -> None:
    
    asset_name = context.asset_key.to_user_string()
    report_date = context.partition_key
    report_url = f"{covid_data_source_url}/{asset_name}/{report_date}.csv"

    context.log.info(f"Ingesting data for {report_date}.")
    context.log.info(f"Report URL: {report_url}.")

    try:
        data = pd.read_csv(report_url)
    except Exception as e:
        context.log.error(f"Error: Unable to fetch or parse the CSV file. Details: {e}")
        context.add_output_metadata({"num_rows": 0})
    else:
        context.log.info(f"Data for {report_date} fetched successfully.")
        context.log.info(f"Data Preview: {data.head()}")
        
        data = rename_columns_to_lowercase(data)
        data = missing_date_column_handler(data, context)
        data = add_ingestion_timestamp(data)
        
        append_data_to_duckdb(context=context,
                              duckdb=duckdb,
                              data=data,
                              schema_name=covid_raw_schema_name,
                              table_name=asset_name)
        
        # Log some metadata about the new table. It will show up in the UI.
        context.add_output_metadata({"num_rows": data.shape[0]})


@dg.asset(
    group_name="daily_reports_ingestion",
    partitions_def=daily_partition,
    kinds={"python", "duckdb"}
)
def csse_covid_19_daily_reports(context: dg.AssetExecutionContext, duckdb: DuckDBResource) -> None:

    """
    Ingests daily COVID-19 Global reports from a specified data source and loads the data into a DuckDB database.

    """
    ingest_data(context, duckdb)


@dg.asset(
    group_name="daily_reports_ingestion",
    partitions_def=daily_partition,
    kinds={"python", "duckdb"}
)
def csse_covid_19_daily_reports_us(context: dg.AssetExecutionContext, duckdb: DuckDBResource) -> None:
    """
    Ingests daily COVID-19 reports for the US from a specified data source and loads the data into a DuckDB database.
    """
    ingest_data(context, duckdb)

