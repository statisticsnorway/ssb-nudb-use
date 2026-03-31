from pathlib import Path

import polars as pl

def _parquet_contains_index_col_0(
    path: Path
) -> bool:
    try:
        columns = pl.Series(pl.scan_parquet(path).collect_schema().names())
    except Exception:
        return False
    return bool((columns == "__index_level_0__").any())


def _select_if_contains_index_col_0(
    path: Path
) -> str:
    if _parquet_contains_index_col_0(path):
        return "* EXCLUDE (__index_level_0__)"
    return "*"
