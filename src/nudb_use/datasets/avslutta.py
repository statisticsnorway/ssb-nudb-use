import duckdb as db

from nudb_use.paths.latest import latest_shared_path
from nudb_use.datasets.utils import _default_alias_from_name, _select_if_contains_index_col_0


def _generate_avslutta_view(alias: str, connection: db.DuckDBPyConnection) -> None:
    last_key, last_path = latest_shared_path("igang")
    if not alias:
        alias = _default_alias_from_name(last_key)
    query = f"""
    CREATE VIEW
        {alias} AS
    SELECT
        {_select_if_contains_index_col_0(last_path, connection)},
        'avslutta' AS nudb_dataset_id
    FROM
        read_parquet('{last_path}')
    """

    connection.sql(query)


def _generate_avslutta_fullfoert_table(
    alias: str, connection: db.DuckDBPyConnection
) -> None:
    from nudb_use.datasets import NudbData
    from nudb_use.variables.derive import (  # type: ignore[attr-defined]
        uh_gruppering_nus,
    )

    query = f"""
        SELECT
            snr,
            nus2000,
            utd_skoleaar_start,
            utd_aktivitet_slutt,
            utd_klassetrinn,
            utd_fullfoertkode,
            CONCAT(nudb_dataset_id, '>avslutta_fullfoert') AS nudb_dataset_id
        FROM
            {NudbData("avslutta").alias}
        WHERE
            utd_fullfoertkode == '8';
    """

    _avslutta_fullfoert_pandas = connection.sql(query).df().pipe(uh_gruppering_nus)

    create_table = f"""
        CREATE TABLE {alias} AS SELECT * FROM _avslutta_fullfoert_pandas
    """

    connection.execute(create_table)
