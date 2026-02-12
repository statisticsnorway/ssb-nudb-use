import duckdb as db

from nudb_use.paths.latest import latest_shared_path
from nudb_use.datasets.utils import _default_alias_from_name, _select_if_contains_index_col_0


def _generate_igang_view(alias: str, connection: db.DuckDBPyConnection) -> None:
    last_key, last_path = latest_shared_path("igang")
    if not alias:
        alias = _default_alias_from_name(last_key)
    query = f"""
    CREATE VIEW
        {alias} AS
    SELECT
        {_select_if_contains_index_col_0(last_path, connection)},
        'igang' AS nudb_dataset_id
    FROM
        read_parquet('{last_path}')
    """

    connection.sql(query)
