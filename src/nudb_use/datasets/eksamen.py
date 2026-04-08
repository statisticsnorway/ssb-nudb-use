import duckdb as db
from nudb_config import settings

from nudb_use.datasets.utils import _default_alias_from_name
from nudb_use.datasets.utils import _nudb_data_select_all
from nudb_use.nudb_logger import logger
from nudb_use.paths.latest import latest_shared_path

VIDEREUTDANNING_UHGRUPPE = settings.constants.videreutd_uhgrupper


def _generate_eksamen_aggregated_view(
    alias: str, connection: db.DuckDBPyConnection
) -> None:
    from nudb_use.datasets.nudb_data import NudbData

    nudb_eksamen = NudbData("eksamen")
    FAILED_KARAKTER_CODES = ["F", "H", "T", "X"]

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
            {nudb_eksamen.alias}
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
            {nudb_eksamen.alias}
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
        CREATE VIEW {alias} AS (
            {query_select_before_2014}
        ) UNION ALL BY NAME(
            {query_aggregate_after_2014}
        );
    """

    logger.debug(f"DUCKDB QUERY:\n{query}")

    connection.sql(query)


def _generate_eksamen_hoeyeste_view(
    alias: str, connection: db.DuckDBPyConnection
) -> None:
    from nudb_use.datasets.nudb_data import NudbData

    # `uh_gruppering_nus` is generated dynamically at runtime

    query_1 = f"""
        SELECT
            T1.snr,
            T1.utd_skoleaar_start,
            T1.uh_eksamen_dato,
            T1.uh_eksamen_studpoeng,
            T1.utd_datakilde,
            T1.nudb_dataset_id,
            T2.utd_klassetrinn_lav_nus AS utd_klassetrinn,
            T2.uh_gruppering_nus,
            uh_gruppering_nus IN {VIDEREUTDANNING_UHGRUPPE} AS is_vidutd,

            CASE WHEN is_vidutd THEN uh_gruppering_nus ELSE '99'
            END AS _uh_gruppering_pool,

            CASE WHEN is_vidutd THEN T1.nus2000
            ELSE CONCAT('6', SUBSTR(T1.nus2000, 2, 3), '99')
            END AS nus2000

        FROM
            {NudbData("eksamen_aggregated").alias} AS T1

        LEFT JOIN
            {NudbData("nuskat").alias} AS T2
        ON
            T1.nus2000 = T2.nus2000

        WHERE
            NOT uh_eksamen_ergjentak AND
            uh_eksamen_studpoeng > 0 AND
            uh_eksamen_studpoeng IS NOT NULL AND
            SUBSTR(T1.nus2000, 1, 1) IN ['6', '7']
    """

    query_2 = f"""
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

                FROM (
                    {query_1}
                )

                GROUP BY
                    snr, _uh_gruppering_pool, utd_skoleaar_start
            )

        )

        WHERE
            (_uh_gruppering_pool != '99' AND uh_eksamen_studpoeng >= 60) OR
            uh_eksamen_studpoeng >= 120;
    """

    logger.debug(f"QUERY:\n{query_2}")
    connection.execute(query_2)


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


def _generate_eksamen_view(alias: str, connection: db.DuckDBPyConnection) -> None:
    last_key, last_path = latest_shared_path("eksamen")
    if not alias:
        alias = _default_alias_from_name(last_key)
    query = f"""
    CREATE VIEW
        {alias} AS
    SELECT
        {_nudb_data_select_all(last_path, connection, 'eksamen')}
    FROM
        read_parquet('{last_path}')
    """

    connection.sql(query)
