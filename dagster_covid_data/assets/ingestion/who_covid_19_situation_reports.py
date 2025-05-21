from settings import covid_base_source_url, covid_raw_schema_name
from dagster_covid_data.utils.duckdb import reload_data_to_duckdb
from dagster_covid_data.utils.data import rename_columns_to_lowercase, add_ingestion_timestamp
from dagster_duckdb import DuckDBResource

import dagster as dg
import duckdb
import os
import pandas as pd
from pathlib import Path


@dg.asset(
    group_name="who_reports_ingestion",
    kinds={"python", "duckdb"}
)
def who_covid_19_sit_rep_time_series(context: dg.AssetExecutionContext, duckdb: DuckDBResource) -> None:
    # Pull data from a CSV
    asset_name = context.asset_key.to_user_string()
    source_url = f"{covid_base_source_url}/{Path(__file__).stem}/{asset_name}/{asset_name}.csv"

    context.log.info(f"Ingesting data for {asset_name}.")
    context.log.info(f"Report URL: {source_url}.")
    
    data = pd.read_csv(source_url)

    data = rename_columns_to_lowercase(data)
    data = add_ingestion_timestamp(data)

    reload_data_to_duckdb(context=context,
                              db_connection=duckdb,
                              data=data,
                              schema_name=covid_raw_schema_name,
                              table_name=asset_name)

    # Log some metadata about the new table. It will show up in the UI.
    context.add_output_metadata({"num_rows": data.shape[0]})