from nudb_use.paths.latest import latest_shared_paths


def _generate_avslutta_view(alias: str, connection) -> None:
    path = latest_shared_paths("avslutta")

    query = f"""
    CREATE VIEW
        {alias} AS
    SELECT * FROM read_parquet('{path}')
    """

    connection.sql(query)