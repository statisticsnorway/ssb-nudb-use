import duckdb as db
import pandas as pd

from nudb_use.nudb_logger import LoggerStack
from nudb_use.nudb_logger import logger


def _generate_bu_igang_table(
    alias: str,
    connection: db.DuckDBPyConnection,
    from_year: int | None = None,
    to_year: int | None = None,
) -> None:
    from nudb_use.datasets import NudbData

    avslutta = NudbData("avslutta")
    eksamen = NudbData("eksamen")
    igang = NudbData("igang")
    utd_hoeyeste = NudbData("utd_hoeyeste")

    years = pd.concat(
        [
            # all years in avslutta
            avslutta.select_distinct("utd_skoleaar_start").df()["utd_skoleaar_start"],
            # all years in igang
            igang.select_distinct("utd_skoleaar_start").df()["utd_skoleaar_start"],
        ]
    )

    yfirst = int(years.min()) if from_year is None else from_year
    ylast = int(years.max()) if to_year is None else to_year

    bu_igang = connection.sql(f"""
        SELECT DISTINCT
            snr
        FROM (
            SELECT DISTINCT snr FROM {igang.alias}    UNION
            SELECT DISTINCT snr FROM {avslutta.alias} UNION
            SELECT DISTINCT snr FROM {eksamen.alias}
        )
        WHERE
            LENGTH(snr) == 7 /* Keep only valid snrs */
    """).df()

    with LoggerStack(f"Generating BU_IGANG_p{yfirst}_p{ylast}..."):

        for year in range(yfirst, ylast + 1):
            logger.info(f"{year}/{ylast}...")

            igang_y = (
                connection.query(f"""
                    SELECT
                        snr,
                        nus2000,
                        utd_hovedaktivitet_prio
                    FROM (
                        SELECT DISTINCT
                            snr,
                            nus2000,
                            UTD_HOVEDAKTIVITET_PRIO(
                                uh_erhovedaktivitet,
                                fa_erhovedaktivitet,
                                vg_erhovedaktivitet
                            ) AS utd_hovedaktivitet_prio
                        FROM
                            {igang.alias}
                        WHERE
                            utd_skoleaar_start = '{year}'
                    )
                    QUALIFY
                        row_number() OVER (
                            PARTITION BY snr
                            ORDER BY
                            utd_hovedaktivitet_prio DESC,
                            nus2000 DESC
                        ) = 1;
                """)
                .df()[["snr", "nus2000"]]
                .rename(columns={"nus2000": f"igang_{year}"})
            )

            bu_y = connection.sql(f"""
                SELECT DISTINCT
                    snr, utd_hoeyeste_nus2000 AS bu_{year}
                FROM (
                    SELECT
                        snr,
                        utd_hoeyeste_nus2000,
                        utd_hoeyeste_aar,
                        MAX(utd_hoeyeste_aar) OVER (PARTITION BY SNR) AS last_utd_hoeyeste_aar
                    FROM
                        {utd_hoeyeste.alias}
                    WHERE
                        LENGTH(snr) = 7 AND
                        utd_hoeyeste_aar <= {year}
                )
                WHERE
                    last_utd_hoeyeste_aar = utd_hoeyeste_aar;
          """).df()

            bu_igang = bu_igang.merge(
                igang_y, how="left", on="snr", validate="1:1"
            ).merge(bu_y, how="left", on="snr", validate="1:1")

    bu_igang = bu_igang.astype("string[pyarrow]")

    query = f"""
        CREATE TABLE {alias} AS SELECT * FROM bu_igang
    """

    connection.execute(query)
