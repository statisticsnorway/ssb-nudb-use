import duckdb as db

from nudb_use.datasets.utils import _default_alias_from_name
from nudb_use.datasets.utils import _nudb_data_select_all
from nudb_use.paths.latest import latest_shared_path


def _generate_avslutta_view(alias: str, connection: db.DuckDBPyConnection) -> None:
    last_key, last_path = latest_shared_path("avslutta")
    if not alias:
        alias = _default_alias_from_name(last_key)
    query = f"""
    CREATE VIEW
        {alias} AS
    SELECT
        {_nudb_data_select_all(last_path, connection, 'avslutta')}
    FROM
        read_parquet('{last_path}')
    """

    connection.sql(query)


def _generate_avslutta_fullfoert_view(
    alias: str, connection: db.DuckDBPyConnection
) -> None:
    from nudb_use.datasets import NudbData

    query = f"""
        CREATE VIEW
            {alias} AS

        SELECT
            T1.*,
            T2.uh_gruppering_nus

        FROM (

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
                {NudbData("avslutta").alias}
            WHERE
                utd_fullfoertkode == '8'

        ) AS T1

        LEFT JOIN
            {NudbData("nuskat").alias} AS T2
        ON
            T1.nus2000 = T2.nus2000;
    """

    connection.execute(query)
