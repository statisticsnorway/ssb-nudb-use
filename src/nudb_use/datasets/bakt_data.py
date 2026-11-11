import duckdb as db

from nudb_use.datasets.nudb_read_parquet import _nudb_read_parquet
from nudb_use.datasets.utils import _default_alias_from_name
from nudb_use.datasets.utils import _nudb_data_select_all
from nudb_use.paths.paths_bakt import _get_latest_path_bakt_data


def _generate_bakt_dataset_view(
    name: str, alias: str, connection: db.DuckDBPyConnection
) -> None:
    last_path = _get_latest_path_bakt_data(name)

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
