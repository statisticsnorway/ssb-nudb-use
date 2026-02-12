import duckdb as db

from nudb_use.nudb_logger import logger
from nudb_use.paths.latest import latest_shared_path
from nudb_use.datasets.utils import _default_alias_from_name, _select_if_contains_index_col_0


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
            CAST(uh_eksamen_er_gjentak AS BOOLEAN) AS uh_eksamen_er_gjentak,
            utd_orgnr,
            bof_orgnrbed,
            uh_antall_deleksamener,
            uh_antall_deleksamener_bestatt,
            fnr,
            uh_eksamen_dato,
            uh_eksamen_karakter,
            uh_eksamen_studpoeng,
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
            CAST(uh_eksamen_er_gjentak AS BOOLEAN) AS uh_eksamen_er_gjentak,
            utd_orgnr,
            bof_orgnrbed,

            COUNT(*) AS uh_antall_deleksamener,
            SUM(UPPER(uh_eksamen_karakter) NOT IN {FAILED_KARAKTER_CODES}) AS uh_antall_deleksamener_bestatt,
            FIRST(fnr) AS fnr,                                     -- may not be unique per snr
            MAX(uh_eksamen_dato) AS uh_eksamen_dato,               -- pick the date of the last exam
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
            uh_eksamen_er_gjentak,
            utd_orgnr,
            bof_orgnrbed,
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


def _generate_eksamen_hoeyeste_table(
    alias: str, connection: db.DuckDBPyConnection
) -> None:
    from nudb_use.datasets.nudb_data import NudbData
    from nudb_use.variables.derive import (  # type: ignore[attr-defined]
        uh_gruppering_nus,
    )

    # `uh_gruppering_nus` is generated dynamically at runtime

    sub_eksamen = connection.sql(f"""
        SELECT
            snr,
            nus2000,
            SUBSTR(nus2000, 1, 1) AS nus2000_nivaa,
            utd_skoleaar_start,
            uh_eksamen_dato,
            uh_eksamen_studpoeng,
            nudb_dataset_id
        FROM
            {NudbData("eksamen_aggregated").alias}
        WHERE
            NOT uh_eksamen_er_gjentak AND
            uh_eksamen_studpoeng > 0 AND
            uh_eksamen_studpoeng IS NOT NULL AND
            nus2000_nivaa IN ['6', '7'];
    """).df()

    sub_eksamen = uh_gruppering_nus(sub_eksamen)
    sub_eksamen["_uh_gruppering_pool"] = (
        sub_eksamen["uh_gruppering_nus"]
        .map(
            {
                "18": "18",
                "19": "19",
                "20": "20",
                "21": "21",
                "22": "22",
                "23": "23",
                "66": "66",
                "67": "67",
            }
        )
        .fillna("99")
    )

    # Per definition all exam records will get 6 as the first digit
    # Even if the exams have nivaa 7. The last two digits should be 99
    # This might be different for bhu...
    sub_eksamen["nus2000"] = "6" + sub_eksamen["nus2000"].str[1:4] + "99"

    valid_eksamen_records = (
        sub_eksamen.sort_values(by="uh_eksamen_studpoeng", ascending=False)
        .groupby(["snr", "_uh_gruppering_pool"], as_index=False)
        .agg(
            {
                "nus2000": "first",  # pick nus value with highest studpoeng within uhgruppe pool
                "uh_eksamen_studpoeng": "sum",
                "uh_eksamen_dato": "max",
                "nudb_dataset_id": "first",
                "utd_skoleaar_start": "max",
                "uh_gruppering_nus": "first",  # This is iffy...
            }
        )
        .query(
            '(_uh_gruppering_pool == "99" & uh_eksamen_studpoeng >= 120) | uh_eksamen_studpoeng >= 60'
        )
        .assign(nudb_dataset_id=lambda d: d["nudb_dataset_id"] + ">eksamen_hoeyeste")
    )

    if not valid_eksamen_records.shape[0]:
        logger.error(f"eksamen_hoeyeste / {alias} is empty!")

    connection.sql(f"CREATE TABLE {alias} AS SELECT * FROM valid_eksamen_records")


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
        {_select_if_contains_index_col_0(last_path, connection)},
        'eksamen' AS nudb_dataset_id
    FROM
        read_parquet('{last_path}')
    """

    connection.sql(query)
