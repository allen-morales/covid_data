from pathlib import Path
import dagster as dg
from dagster_duckdb import DuckDBResource

from settings import dbt_resource, DUCK_DATABASE_PATH
import os

import dagster_covid_data.assets as assets
from dagster_covid_data.assets.dbt_assets import dbt_models

assets = dg.load_assets_from_modules([assets])

# Dagster object that contains the dbt assets and resource
# Add Dagster definitions to Definitions object
defs = dg.Definitions(
    assets=assets,
    resources={'dbt': dbt_resource,
               'duckdb': DuckDBResource(database=os.fspath(DUCK_DATABASE_PATH))
            }
)