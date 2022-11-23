from dagster import load_assets_from_package_module, repository

from duckpond import assets

from duckpond.datapond import DuckDB
from dagster import resource

@resource(config_schema={"vars": str})
def duckdb(init_context):
    return DuckDB(init_context.resource_config["vars"])

duckdb_localstack = duckdb.configured(
    {
        "vars": """
set s3_access_key_id='test';
set s3_secret_access_key='test';
set s3_endpoint='localhost:4566';
set s3_use_ssl='false';
set s3_url_style='path';
"""
    }
)

from duckpond.duckpond import DuckPondIOManager
from dagster import io_manager

@io_manager(required_resource_keys={"duckdb"})
def duckpond_io_manager(init_context):
    return DuckPondIOManager("datalake", init_context.resources.duckdb)

    from dagster import (
    load_assets_from_package_module,
    repository,
    with_resources,
)
from jaffle import assets

@repository
def duckpond():
    return [
        with_resources(
            load_assets_from_package_module(assets),
            {"io_manager": duckpond_io_manager, "duckdb": duckdb_localstack},
        )
    ]
