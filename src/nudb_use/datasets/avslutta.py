import duckdb as db
import polars as pl

from nudb_use.datasets.utils import _select_if_contains_index_col_0
from nudb_use.paths.latest import latest_shared_path


def _generate_avslutta_view() -> pl.LazyFrame:
    last_key, last_path = latest_shared_path("avslutta")

    query = f"""
    SELECT
        {_select_if_contains_index_col_0(last_path)}
    FROM
        read_parquet('{last_path}')
    """

    data_id = pl.col("nudb_dataset_id").fill_null(pl.lit("")) + pl.lit(">avslutta")
    
    return (
        pl.sql(query)
        .with_columns(nudb_dataset_id = data_id)
    )


def _generate_avslutta_fullfoert_view() -> pl.LazyFrame:
    from nudb_use.datasets import NudbData
    from nudb_use.variables.derive import (  # type: ignore[attr-defined]
        uh_gruppering_nus,
    )

    avslutta = NudbData("avslutta")
    
    fullfoert = pl.SQLContext(avslutta=avslutta.data).execute(f"""
        SELECT
            snr,
            nus2000,
            utd_skoleaar_start,
            utd_aktivitet_slutt,
            utd_klassetrinn,
            utd_fullfoertkode,
            utd_datakilde,
            CONCAT(nudb_dataset_id, '>avslutta_fullfoert') AS nudb_dataset_id
        FROM
            avslutta
        WHERE
            utd_fullfoertkode == '8';
    """)

    return uh_gruppering_nus(fullfoert)