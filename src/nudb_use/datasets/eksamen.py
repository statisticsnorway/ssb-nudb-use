import duckdb as db
import polars as pl
from nudb_config import settings

from nudb_use.datasets.utils import _select_if_contains_index_col_0
from nudb_use.nudb_logger import logger
from nudb_use.paths.latest import latest_shared_path

VIDEREUTDANNING_UHGRUPPE = settings.constants.videreutd_uhgrupper


def _generate_eksamen_aggregated_view() -> pl.LazyFrame:
    from nudb_use.datasets.nudb_data import NudbData

    nudb_eksamen = NudbData("eksamen")
    FAILED_KARAKTER_CODES = ("F", "H", "T", "X")

    # We have to review this logic a bit
    # this is quite a naive approach
    # we should atleast split this by year, such that we don't
    # attempt to aggregate aarganger where the aggregation has already been done
    # It's pretty close, but we may need to
    query_select_before_2014 = f"""
        SELECT
            snr,
            nus2000,
            uh_institusjon_id,
            utd_skoleaar_start,
            uh_studienivaa,
            CAST(uh_eksamen_ergjentak AS BOOLEAN) AS uh_eksamen_ergjentak,
            orgnrbed,
            orgnr_foretak,
            uh_antall_deleksamener,
            uh_antall_deleksamener_bestatt,
            fnr,
            uh_eksamen_dato,
            uh_eksamen_karakter,
            uh_eksamen_studpoeng,
            utd_datakilde,
            CONCAT(nudb_dataset_id, '>eksamen_aggregated') AS nudb_dataset_id
        FROM
            EKSAMEN
        WHERE
            utd_skoleaar_start < '2014'
    """

    query_aggregate_after_2014 = f"""
        SELECT
            snr,
            nus2000,
            uh_institusjon_id,
            utd_skoleaar_start,
            uh_studienivaa,
            CAST(uh_eksamen_ergjentak AS BOOLEAN) AS uh_eksamen_ergjentak,
            orgnrbed,
            orgnr_foretak,

            COUNT(*) AS uh_antall_deleksamener,
            SUM(UPPER(uh_eksamen_karakter) NOT IN {FAILED_KARAKTER_CODES}) AS uh_antall_deleksamener_bestatt,
            FIRST(fnr) AS fnr,                                     -- may not be unique per snr
            MAX(uh_eksamen_dato) AS uh_eksamen_dato,               -- pick the date of the last exam
            FIRST(utd_datakilde) as utd_datakilde,
            FIRST(uh_eksamen_karakter) AS uh_eksamen_karakter,     -- Think we used to just pick random before. Carl: 'Using `MAX(uh_eksamen_karakter)` is too correct'
            SUM(uh_eksamen_studpoeng) AS uh_eksamen_studpoeng,
            CONCAT(FIRST(nudb_dataset_id), '>eksamen_aggregated') AS nudb_dataset_id
        FROM
            EKSAMEN
        WHERE
            utd_skoleaar_start >= '2014'
        GROUP BY
            snr,
            nus2000,
            uh_institusjon_id,
            utd_skoleaar_start,
            uh_studienivaa,
            uh_eksamen_ergjentak,
            orgnrbed,
            orgnr_foretak,
            EXTRACT(YEAR FROM uh_eksamen_dato) -- used to seperate semesters
    """
    query = f"""
        (
            {query_select_before_2014}
        ) UNION ALL BY NAME (
            {query_aggregate_after_2014}
        );
    """

    logger.debug(f"SQL QUERY:\n{query}")

    return pl.SQLContext(EKSAMEN=nudb_eksamen.data).execute(query)


def _generate_eksamen_hoeyeste_view() -> pl.LazyFrame:
    from nudb_use.datasets.nudb_data import NudbData
    from nudb_use.variables.derive import (  # type: ignore[attr-defined]
        uh_gruppering_nus,
    )
    from nudb_use.variables.derive import utd_klassetrinn_lav_nus

    # `uh_gruppering_nus` is generated dynamically at runtime
    eksamen_aggregated = NudbData("eksamen_aggregated")
    
    sub_eksamen = (
        pl.SQLContext(EKSAMEN_AGGREGATED=eksamen_aggregated.data)
        .execute(f"""
            SELECT
                snr,
                nus2000,
                utd_skoleaar_start,
                uh_eksamen_dato,
                uh_eksamen_studpoeng,
                utd_datakilde,
                nudb_dataset_id
            FROM
                EKSAMEN_AGGREGATED
            WHERE
                NOT uh_eksamen_ergjentak AND
                uh_eksamen_studpoeng > 0 AND
                uh_eksamen_studpoeng IS NOT NULL AND
                SUBSTR(nus2000, 1, 1) IN ('6', '7');
        """
        )
    )

    # Derive utd_klassetrinn from nus2000 and klass (lowest)
    sub_eksamen = utd_klassetrinn_lav_nus(sub_eksamen).rename(
        {"utd_klassetrinn_lav_nus": "utd_klassetrinn"}
    )

    # Derive uh_gruppering
    sub_eksamen = (
        uh_gruppering_nus(sub_eksamen)
        .with_columns(
            is_vidutd = pl.col("_uh_gruppering_pool").is_in(VIDEREUTDANNING_UHGRUPPE),
            _uh_gruppering_pool = (
                pl.when(pl.col("is_vidutd"))
                .then(pl.col("uh_gruppering_nus"))
                .otherwise(pl.lit("99"))
            ),
            nus2000 = (
                pl.when(pl.col("is_vidutd"))
                .then(pl.col("nus2000"))
                .otherwise(pl.lit("6") + pl.col("nus2000").str.slice(1, 3) + pl.lit("99"))
            )
        )
    )

    return pl.SQLContext(SUB_EKSAMEN=sub_eksamen).execute(f"""
        CREATE TABLE
            {alias} AS
        SELECT
            snr,
            SUBSTR(_sp_nus2000, 7) AS nus2000,
            uh_eksamen_studpoeng,
            nudb_dataset_id,
            uh_eksamen_dato,
            utd_skoleaar_start,
            uh_gruppering_nus,
            utd_datakilde,
            utd_klassetrinn,
            _uh_gruppering_pool

        FROM (

            SELECT
                -- Get the current nus2000 value with the most studpoeng
                MAX( -- Use LPAD() in conjunction with SUBSTR() to assure a fixed width and format
                    CONCAT(SUBSTR(LPAD(CAST(uh_eksamen_studpoeng AS VARCHAR), 6, '0'), 1, 6), -- '22.7' -> '0022.7', '180.0' -> '0180.0'
                           SUBSTR(LPAD(nus2000, 6, '0'), 1, 6)
                )) OVER (
                    PARTITION BY snr, _uh_gruppering_pool
                    ORDER BY utd_skoleaar_start
                ) AS _sp_nus2000,

                -- Get the cumulative studpoeng
                SUM(uh_eksamen_studpoeng) OVER (
                    PARTITION BY snr, _uh_gruppering_pool
                    ORDER BY utd_skoleaar_start
                ) AS uh_eksamen_studpoeng,

                CONCAT(nudb_dataset_id, '>eksamen_hoeyeste') AS nudb_dataset_id,

                snr,
                uh_eksamen_dato,
                utd_skoleaar_start,
                uh_gruppering_nus,
                utd_datakilde,
                utd_klassetrinn,
                _uh_gruppering_pool

            FROM (

                SELECT
                    snr,
                    _uh_gruppering_pool,
                    utd_skoleaar_start,


                    FIRST(nus2000 ORDER BY uh_eksamen_studpoeng) AS nus2000,
                    FIRST(utd_datakilde ORDER BY uh_eksamen_studpoeng) AS utd_datakilde,
                    FIRST(utd_klassetrinn ORDER BY uh_eksamen_studpoeng) AS utd_klassetrinn,
                    SUM(uh_eksamen_studpoeng) AS uh_eksamen_studpoeng,
                    MAX(uh_eksamen_dato) AS uh_eksamen_dato,
                    FIRST(uh_gruppering_nus ORDER BY uh_eksamen_studpoeng) AS uh_gruppering_nus,
                    FIRST(nudb_dataset_id) AS nudb_dataset_id

                FROM
                    SUB_EKSAMEN

                GROUP BY
                    snr, _uh_gruppering_pool, utd_skoleaar_start
            )

        )

        WHERE
            (_uh_gruppering_pool != '99' AND uh_eksamen_studpoeng >= 60) OR
            uh_eksamen_studpoeng >= 120;
    """)


def _generate_eksamen_avslutta_hoeyeste_view(
    alias: str, connection: db.DuckDBPyConnection
) -> None:
    from nudb_use.datasets.nudb_data import NudbData

    query = f"""
        CREATE VIEW {alias} AS (
            SELECT
                snr,
                nus2000,
                utd_skoleaar_start,
                uh_eksamen_dato,
                uh_eksamen_studpoeng,
                uh_gruppering_nus,
                utd_datakilde,
                utd_klassetrinn,
                CONCAT(nudb_dataset_id, '>eksamen_avslutta_hoeyeste') AS nudb_dataset_id
            FROM
                {NudbData("eksamen_hoeyeste").alias}
        ) UNION ALL BY NAME (
            SELECT
                snr,
                nus2000,
                utd_skoleaar_start,
                utd_aktivitet_slutt,
                utd_klassetrinn,
                uh_gruppering_nus,
                utd_datakilde,
                CONCAT(nudb_dataset_id, '>eksamen_avslutta_hoeyeste') AS nudb_dataset_id
            FROM
                {NudbData("avslutta_fullfoert").alias}
            WHERE
                utd_fullfoertkode == '8'
        )

    """

    connection.sql(query)


def _generate_eksamen_view() -> pl.LazyFrame:
    last_key, last_path = latest_shared_path("eksamen")

    query = f"""
    SELECT
        {_select_if_contains_index_col_0(last_path)},
        'eksamen' AS nudb_dataset_id
    FROM
        read_parquet('{last_path}')
    """

    return pl.sql(query)
