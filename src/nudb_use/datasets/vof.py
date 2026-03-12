import duckdb as db
from pathlib import Path
from nudb_use.paths.latest import latest_shared_paths


def _generate_vof_unique_orgnrbed(
    alias: str,
    connection: db.DuckDBPyConnection,
) -> None:
    """All the unique orgnrbed ever."""
    paths = latest_shared_paths("vof_situttak")

    paths_dict: dict[str, Path]
    if isinstance(paths, Path):
        paths_dict = {"vof_situttak": paths}
    else:
        paths_dict = paths

    # Later datasets first.
    paths_dict = {
        key: paths_dict[key]
        for key in sorted(paths_dict, reverse=True)
    }

    union_parts: list[str] = []
    for path in paths_dict.values():
        path_str = str(path).replace("'", "''")
        union_parts.append(
            f"""
            SELECT DISTINCT
                orgnrbed
            FROM read_parquet('{path_str}')
            """
        )

    union_sql = "\nUNION ALL\n".join(union_parts)

    query = f"""
        CREATE OR REPLACE VIEW {alias} AS
        SELECT DISTINCT orgnrbed
        FROM ({union_sql})
        WHERE orgnrbed IS NOT NULL AND TRIM(CAST(orgnrbed AS VARCHAR)) != ''
        ;
    """

    connection.sql(query)