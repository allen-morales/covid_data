"""
settings.py

This module defines global configuration, directory paths, and resource objects for the covid_data project.

Contents:
    - Project and data directory paths
    - DuckDB database path
    - dbt project and resource configuration
    - Source URLs for COVID-19 datasets

Attributes:
    PROJECT_DIRECTORY (Path): Root directory of the project.
    DATA_DIRECTORY (Path): Directory for storing data files.
    DUCK_DATABASE_PATH (Path): Path to the DuckDB database file.
    DBT_PROJECT_PATH (Path): Path to the dbt project directory.
    dbt_project (DbtProject): dbt project configuration object.
    dbt_resource (DbtCliResource): Dagster resource for running dbt CLI commands.
    covid_data_source_url (str): URL for raw COVID-19 data.
    covid_base_source_url (str): Base URL for COVID-19 data repository.
    covid_raw_schema_name (str): Default schema name for raw COVID-19 data.
"""

from pathlib import Path
from dagster_dbt import (
    DbtCliResource,
    DbtProject
)

#Paths
PROJECT_DIRECTORY = Path(__file__).parent.resolve()
DATA_DIRECTORY = Path.joinpath(PROJECT_DIRECTORY, "data")
DUCK_DATABASE_PATH = Path.joinpath(DATA_DIRECTORY, "covid.duckdb")
DBT_PROJECT_PATH = Path.joinpath(PROJECT_DIRECTORY, "dbt_covid_data")

# DBT Settings
dbt_project = DbtProject(project_dir=DBT_PROJECT_PATH,
                         profiles_dir=DBT_PROJECT_PATH)

dbt_project.prepare_if_dev()

dbt_resource = DbtCliResource(project_dir=dbt_project)


# Source URLs
covid_data_source_url = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data"
covid_base_source_url = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/"
covid_raw_schema_name = "covid_data_raw"