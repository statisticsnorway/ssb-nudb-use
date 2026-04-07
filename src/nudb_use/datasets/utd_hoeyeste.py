import duckdb as db
import pandas as pd

from nudb_use.nudb_logger import logger


def _generate_utd_hoeyeste_view(
    alias: str,
    connection: db.DuckDBPyConnection,
    first_year: int = 1970,
    last_year: int | None = None,
    valid_snrs: None | pd.Series = None,
) -> None:
    from nudb_use.datasets import NudbData

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

    last_year = last_year_data if last_year is None else max(last_year, last_year_data)

    logger.info(f"Deriving `utd_hoeyeste` [{first_year}-{last_year}] as view.")

    query = f"""
        CREATE OR REPLACE VIEW {alias} AS
        WITH base AS (
            SELECT
                snr,
                nus2000,
                COALESCE(utd_aktivitet_slutt, uh_eksamen_dato) AS utd_hoeyeste_dato,
                utd_hoeyeste_rangering(
                    nus2000,
                    uh_eksamen_dato,
                    uh_eksamen_studpoeng,
                    uh_gruppering_nus,
                    utd_aktivitet_slutt,
                    utd_klassetrinn,
                    utd_skoleaar_start
                ) AS utd_hoeyeste_rangering,
                utd_datakilde,
                utd_klassetrinn
            FROM
                {eksamen_avslutta_hoeyeste.alias}
        ),
        years AS (
            SELECT
                year::VARCHAR AS utd_hoeyeste_aar,
                MAKE_DATE(year, 10, 1) AS cutoff_date
            FROM generate_series({first_year}, {last_year}) AS t(year)
        ),
        ranked AS (
            SELECT
                y.utd_hoeyeste_aar,
                b.snr,
                b.nus2000,
                b.utd_datakilde,
                b.utd_klassetrinn,
                b.utd_hoeyeste_rangering,
                ROW_NUMBER() OVER (
                    PARTITION BY y.utd_hoeyeste_aar, b.snr
                    ORDER BY
                        b.utd_hoeyeste_rangering DESC,
                        (b.utd_klassetrinn IS NOT NULL) DESC,
                        (b.utd_datakilde IS NOT NULL) DESC,
                        b.utd_hoeyeste_dato DESC,
                        b.nus2000 DESC
                ) AS rn
            FROM years y
            JOIN base b
              ON b.utd_hoeyeste_dato <= y.cutoff_date
        )
        SELECT
            snr,
            nus2000,
            utd_hoeyeste_rangering,
            utd_hoeyeste_aar,
            utd_datakilde,
            utd_klassetrinn
        FROM ranked
        WHERE rn = 1
    """

    connection.execute(query)
