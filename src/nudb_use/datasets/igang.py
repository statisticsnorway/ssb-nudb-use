import duckdb as db

from nudb_use.datasets.nudb_read_parquet import _nudb_read_parquet
from nudb_use.datasets.utils import _default_alias_from_name
from nudb_use.datasets.utils import _nudb_data_select_all
from nudb_use.paths.latest import latest_shared_path


def _generate_igang_view(alias: str, connection: db.DuckDBPyConnection) -> None:
    last_key, last_path = latest_shared_path("igang")
    if not alias:
        alias = _default_alias_from_name(last_key)
    query = f"""
    CREATE VIEW
        {alias} AS
    SELECT
        {_nudb_data_select_all(last_path, connection, 'igang')}
    FROM
        {_nudb_read_parquet(last_path, alias)}
    """

    connection.sql(query)
