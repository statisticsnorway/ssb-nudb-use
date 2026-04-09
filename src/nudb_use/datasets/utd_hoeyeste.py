import duckdb as db

from nudb_use.nudb_logger import logger


def _generate_utd_hoeyeste_last_view(
    alias: str,
    connection: db.DuckDBPyConnection,
) -> None:
    from nudb_use.datasets import NudbData  # Avoids circular import

    utd_hoyeste_view = NudbData("utd_hoeyeste")
    logger.info("Deriving `utd_hoyeste_last` as view - latest, highest education.")
    query = f"""
    CREATE VIEW {alias} AS
    SELECT * FROM (
        SELECT
            *,
            MAX(utd_hoeyeste_aar) OVER (PARTITION BY SNR) AS last_utd_hoeyeste_aar
        FROM
            {utd_hoyeste_view.alias}
        WHERE
            LENGTH(snr) = 7
    )

    WHERE
        last_utd_hoeyeste_aar = utd_hoeyeste_aar;
    """
    connection.execute(query)


def _generate_utd_hoeyeste_view(
    alias: str,
    connection: db.DuckDBPyConnection,
) -> None:
    from nudb_use.datasets import NudbData  # Avoids circular import

    eksamen_avslutta_hoeyeste = NudbData("eksamen_avslutta_hoeyeste")

    # Find the latest year present in the source after computing the derived date
    last_year_data = connection.sql(f"""
        SELECT
            EXTRACT(
                YEAR FROM MAX(COALESCE(utd_aktivitet_slutt, uh_eksamen_dato))
            )::INTEGER AS last_year_data
        FROM
            {eksamen_avslutta_hoeyeste.alias};
        """).df()["last_year_data"].iloc[0]

    if last_year_data is None:
        raise ValueError("No data found in `eksamen_avslutta_hoeyeste`.")

    logger.info(
        f"Deriving `utd_hoeyeste` with the last year being: {last_year_data} as view."
    )

    query = f"""
        CREATE VIEW {alias} AS

        WITH T0 AS (
            SELECT
                snr,
                nus2000,
                COALESCE(utd_aktivitet_slutt, uh_eksamen_dato) AS utd_hoeyeste_dato,
                UTD_HOEYESTE_RANGERING(
                    nus2000,
                    uh_eksamen_dato,
                    uh_eksamen_studpoeng,
                    uh_gruppering_nus,
                    utd_aktivitet_slutt,
                    utd_klassetrinn,
                    utd_skoleaar_start
                ) AS utd_hoeyeste_rangering,
                utd_datakilde,
                utd_klassetrinn,
                UTD_HOEYESTE_AAR(utd_hoeyeste_dato) AS utd_hoeyeste_aar
            FROM
                {eksamen_avslutta_hoeyeste.alias}
        ),

        T1 AS (
            SELECT
                *,
                MAX(utd_hoeyeste_rangering) OVER (
                    PARTITION BY snr
                    ORDER BY utd_hoeyeste_aar
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS cmax_utd_hoeyeste_rangering
            FROM
                T0
        )

        SELECT
            snr,
            utd_datakilde,
            utd_klassetrinn,
            utd_hoeyeste_dato,
            utd_hoeyeste_aar,
            utd_hoeyeste_rangering,
            nus2000 AS utd_hoeyeste_nus2000
        FROM
            T1
        WHERE
            utd_hoeyeste_rangering==cmax_utd_hoeyeste_rangering
    """

    connection.execute(query)
