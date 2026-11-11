import duckdb as db

from nudb_use.datasets.nudb_read_parquet import _nudb_read_parquet
from nudb_use.datasets.utils import _default_alias_from_name
from nudb_use.datasets.utils import _nudb_data_select_all
from nudb_use.paths.paths_baked import _get_latest_path_baked_data

USE_BAKED_DATASETS = True


def _set_use_baked_datasets() -> None:
    global USE_BAKED_DATASETS
    USE_BAKED_DATASETS = True


def _set_ignore_baked_datasets() -> None:
    global USE_BAKED_DATASETS
    USE_BAKED_DATASETS = False


def _use_baked_datasets() -> bool:
    global USE_BAKED_DATASETS
    return USE_BAKED_DATASETS


def _generate_baked_dataset_view(
    name: str, alias: str, connection: db.DuckDBPyConnection
) -> None:
    last_path = _get_latest_path_baked_data(name)

    if not alias:
        alias = _default_alias_from_name(name)

    query = f"""
        CREATE VIEW
            {alias} AS
        SELECT
            {_nudb_data_select_all(last_path, connection, name)}
        FROM
            {_nudb_read_parquet(last_path, alias)}
    """

    connection.sql(query)
