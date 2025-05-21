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