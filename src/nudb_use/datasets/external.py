from functools import partial

import duckdb as db
from nudb_config import settings

from nudb_use.datasets.utils import _default_alias_from_name
from nudb_use.paths.latest import latest_shared_paths

__all__ = []

EXTERNAL_DATASETS = [
    dataset_name
    for dataset_name, dataset_values in settings.dataset.items()
    if dataset_values.team != settings.dapla_team
]


def _generate_view(
    dataset_name: str, alias: str, connection: db.DuckDBPyConnection
) -> None:
    path = latest_shared_paths(dataset_name)
    alias = _default_alias_from_name(dataset_name)
    query = f"""
    CREATE VIEW
        {alias} AS
    SELECT
        *,
        '{dataset_name}' AS nudb_dataset_id
    FROM
        read_parquet('{path}')
    """

    connection.sql(query)


for dataset_name in EXTERNAL_DATASETS:
    function_name = f"_generate_{dataset_name}_view"
    globals()[function_name] = partial(_generate_view, dataset_name=dataset_name)
    __all__.append(function_name)
