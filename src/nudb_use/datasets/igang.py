import duckdb as db

from nudb_use.paths.latest import latest_shared_paths


def _generate_igang_view(alias: str, connection: db.DuckDBPyConnection) -> None:
    path = latest_shared_paths("igang")

    query = f"""
    CREATE VIEW
        {alias} AS
    SELECT
        *,
        'igang' AS nudb_dataset_id
    FROM
        read_parquet('{path}')
    """

    connection.sql(query)
