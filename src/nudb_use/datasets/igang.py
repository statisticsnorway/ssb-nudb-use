import polars as pl

from nudb_use.datasets.utils import _select_if_contains_index_col_0
from nudb_use.paths.latest import latest_shared_path


def _generate_igang_view() -> pl.LazyFrame:
    last_key, last_path = latest_shared_path("igang")

    query = f"""
    CREATE VIEW
        {alias} AS
    SELECT
        {_select_if_contains_index_col_0(last_path)},
        'igang' AS nudb_dataset_id
    FROM
        read_parquet('{last_path}')
    """

    return pl.sql(query)
