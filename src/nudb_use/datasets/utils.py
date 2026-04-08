from pathlib import Path

import duckdb as db


def _default_alias_from_name(name: str) -> str:
    return f"NUDB_DATA_{name.upper().replace('-','_')}"


def _parquet_columns(path: Path, connection: db.DuckDBPyConnection) -> set[str]:
    try:
        columns = connection.sql(f"DESCRIBE SELECT * FROM read_parquet('{path}')").df()[
            "column_name"
        ]
    except Exception:
        return {}

    return set(columns)


def _nudb_data_select_all(
    path: Path,
    connection: db.DuckDBPyConnection,
    dataset: str | None = None,
    exclude: tuple[str] | None = ["__index_level_0__", "nudb_dataset_id"],
) -> str:

    cols = _parquet_columns(path, connection)
    exclude_in_cols = () if exclude is None else tuple(set(exclude) & cols)
    excluding_id = "nudb_dataset_id" in exclude_in_cols

    select = "*"

    if exclude_in_cols:
        select += f"EXCLUDE {exclude_in_cols}"

    if excluding_id and dataset:
        select += f",\nCONCAT(nudb_dataset_id, '>{dataset}') AS nudb_dataset_id"

    elif excluding_id:
        select += ",\nnudb_dataset_id"

    elif "nudb_dataset_id" not in cols and dataset:
        select += f",\n'{dataset}' AS nudb_dataset_id"

    return select
