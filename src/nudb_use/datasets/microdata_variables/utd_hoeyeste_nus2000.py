import duckdb as db

from nudb_use.nudb_logger import logger


def _generate_microdata_utd_hoeyeste_nus2000_view(
    alias: str,
    connection: db.DuckDBPyConnection,
) -> None:
    from nudb_use.datasets import NudbData  # Avoids circular import

    utd_hoeyeste_view = NudbData("utd_hoeyeste")
    logger.info("Deriving `utd_hoeyeste_nus2000` Microdata variable.")

    query = f"""
        CREATE OR REPLACE VIEW _TEMP_CATALOGUE AS (
            SELECT
                snr, utd_hoeyeste_nus2000, utd_hoeyeste_dato,
                ROW_NUMBER() OVER (PARTITION BY snr ORDER BY utd_hoeyeste_dato ASC) AS index
            FROM
                {utd_hoeyeste_view.alias}
        );

        CREATE OR REPLACE VIEW {alias} AS (
            SELECT
                T1.snr AS snr,
                T1.utd_hoeyeste_nus2000 AS utd_hoeyeste_nus2000,
                T1.utd_hoeyeste_dato AS gyldig_fra_dato,
                T2.utd_hoeyeste_dato AS gyltid_til_dato
            FROM
                _TEMP_CATALOGUE AS T1
            LEFT JOIN
                _TEMP_CATALOGUE AS T2
            ON
                T1.snr = T2.snr AND
                T1.index + 1 = T2.index
        );

    """

    connection.execute(query)
