from functools import partial

import duckdb as db
from nudb_config import settings

from nudb_use import LoggerStack
from nudb_use.datasets.utils import _default_alias_from_name
from nudb_use.datasets.utils import _select_if_contains_index_col_0
from nudb_use.paths.latest import latest_shared_path

__all__ = []

EXTERNAL_DATASETS = [
    dataset_name
    for dataset_name, dataset_values in settings.datasets.items()
    if dataset_values.team != settings.dapla_team
]


def _generate_view(
    dataset_name: str, alias: str, connection: db.DuckDBPyConnection
) -> None:
    with LoggerStack(f"Creating a view for {dataset_name} with alias of {alias}."):
        last_key, last_path = latest_shared_path(dataset_name)

        if not alias:
            alias = _default_alias_from_name(last_key)

        query = f"""
        CREATE VIEW
            {alias} AS
        SELECT
            {_select_if_contains_index_col_0(last_path, connection)},
            '{dataset_name}' AS nudb_dataset_id
        FROM
            read_parquet('{last_path}')

        """

        connection.sql(query)


for dataset_name in EXTERNAL_DATASETS:
    function_name = f"_generate_{dataset_name}_view"
    globals()[function_name] = partial(_generate_view, dataset_name=dataset_name)
    __all__.append(function_name)
