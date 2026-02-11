from nudb_config import settings

import duckdb as db

from nudb_use.paths.latest import latest_shared_paths
from functools import partial

__all__ = []

def _generate_view(dataset_name: str, connection: db.DuckDBPyConnection) -> None:
    path = latest_shared_paths(dataset_name)

    query = f"""
    CREATE VIEW
        {dataset_name} AS
    SELECT
        *,
        '{dataset_name}' AS nudb_dataset_id
    FROM
        read_parquet('{path}')
    """

    connection.sql(query)


external_datasets = [dataset_name
                     for dataset_name, dataset_values 
                     in settings.dataset.items() 
                     if dataset_values.team != settings.dapla_team]

for dataset_name in external_datasets:
    function_name = f"_generate_{dataset_name}_view"
    globals()[function_name] = partial(_generate_view, dataset_name=dataset_name)
    __all__.append(function_name)


