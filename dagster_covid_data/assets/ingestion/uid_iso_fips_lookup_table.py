from settings import covid_data_source_url, covid_raw_schema_name
from dagster_covid_data.utils.duckdb import reload_data_to_duckdb
from dagster_covid_data.utils.data import rename_columns_to_lowercase, add_ingestion_timestamp
from dagster_duckdb import DuckDBResource

import dagster as dg
import duckdb
import os
import pandas as pd


@dg.asset(
    group_name="git_hub_ingestion",
    kinds={"python", "duckdb"}
)
def uid_iso_fips_lookup_table(context: dg.AssetExecutionContext, duckdb: DuckDBResource) -> None:
    # Pull data from a CSV
    asset_name = context.asset_key.to_user_string()
    source_url = f"{covid_data_source_url}/UID_ISO_FIPS_LookUp_Table.csv"
    
    context.log.info(f"Ingesting data for {asset_name}.")
    context.log.info(f"Report URL: {source_url}.")
    
    data = pd.read_csv(source_url)

    data = rename_columns_to_lowercase(data)
    data = add_ingestion_timestamp(data)

    reload_data_to_duckdb(context=context,
                              duckdb=duckdb,
                              data=data,
                              schema_name=covid_raw_schema_name,
                              table_name=asset_name)

    # Log some metadata about the new table. It will show up in the UI.
    context.add_output_metadata({"num_rows": data.shape[0]})