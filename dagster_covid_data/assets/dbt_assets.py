from settings import dbt_project

from dagster_dbt import (
    DbtCliResource,
    dbt_assets
)

import dagster as dg

# Yields Dagster events streamed from the dbt CLI
@dbt_assets(manifest=dbt_project.manifest_path)
def dbt_models(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
