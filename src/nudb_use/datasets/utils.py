from pathlib import Path

import duckdb as db


def _default_alias_from_name(name: str) -> str:
    return f"NUDB_DATA_{name.upper().replace('-','_')}"


def _parquet_contains_index_col_0(
    path: Path, connection: db.DuckDBPyConnection
) -> bool:
    try:
        columns = connection.sql(f"DESCRIBE SELECT * FROM read_parquet('{path}')").df()[
            "column_name"
        ]
    except Exception:
        return False
    return bool((columns == "__index_level_0__").any())


def _select_if_contains_index_col_0(
    path: Path, connection: db.DuckDBPyConnection
) -> str:
    if _parquet_contains_index_col_0(path, connection):
        return "* EXCLUDE (__index_level_0__)"
    return "*"
