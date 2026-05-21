import datetime
from collections.abc import Iterable
from functools import lru_cache
from pathlib import Path

import duckdb as db
from fagfunksjoner.paths.versions import get_latest_fileversions
from nudb_config import settings

from nudb_use.nudb_logger import logger
from nudb_use.paths.path_parse import get_periods_from_path
from nudb_use.variables.checks import pyarrow_columns_from_metadata

UNION_ALL = "\nUNION ALL\n"


def _bof_latest_orgnr_placement_ctes_sql(
    relevant_orgnr_cte: str | None = None,
) -> str | None:
    """Return CTE SQL for latest BOF placement of each orgnr."""
    paths = _get_all_bof_situttak_october_paths()
    union_parts: list[str] = []
    for path in paths:
        path_str = str(path).replace("'", "''")
        path_period = _first_date_from_path_period(path).strftime(r"%Y-%m-%d")

        union_parts.append(f"""
            SELECT DISTINCT
                CAST(orgnrbed AS VARCHAR) AS orgnr,
                'orgnrbed' AS orgnr_type,
                CAST('{path_period}' AS DATE) AS bof_period_date
            FROM read_parquet('{path_str}')

            UNION ALL

            SELECT DISTINCT
                CAST(org_nr AS VARCHAR) AS orgnr,
                'foretak' AS orgnr_type,
                CAST('{path_period}' AS DATE) AS bof_period_date
            FROM read_parquet('{path_str}')
            """)

    if not union_parts:
        return None

    relevant_orgnr_filter = (
        ""
        if relevant_orgnr_cte is None
        else f"AND orgnr IN (SELECT orgnr FROM {relevant_orgnr_cte})"
    )
    placements_sql = UNION_ALL.join(union_parts)
    return f"""
        placements AS (
            {placements_sql}
        ),

        latest_placement AS (
            SELECT
                orgnr,
                orgnr_type
            FROM (
                SELECT
                    orgnr,
                    orgnr_type,
                    ROW_NUMBER() OVER (
                        PARTITION BY orgnr
                        ORDER BY
                            bof_period_date DESC,
                            CASE orgnr_type
                                WHEN 'orgnrbed' THEN 1
                                ELSE 0
                            END DESC
                    ) AS placement_rank
                FROM placements
                WHERE
                    orgnr IS NOT NULL AND
                    TRIM(orgnr) != '' AND
                    orgnr != '000000000'
                    {relevant_orgnr_filter}
            )
            WHERE placement_rank = 1
        )
    """


def _create_empty_view(
    alias: str,
    connection: db.DuckDBPyConnection,
    columns: dict[str, str],
) -> None:
    select_list = ",\n            ".join(
        f"CAST(NULL AS {dtype}) AS {name}" for name, dtype in columns.items()
    )
    connection.sql(f"""
        CREATE OR REPLACE VIEW {alias} AS
        SELECT
            {select_list}
        WHERE FALSE
        ;
    """)


def _generate_bof_eierforhold_view(
    alias: str,
    connection: db.DuckDBPyConnection,
) -> None:

    want_cols_pre2014 = ("org_nr", "sektor")
    paths_pre2014 = _get_all_bof_situttak_october_paths(want_cols_pre2014)
    want_cols_post2014 = (
        "org_nr",
        "orgnrbed",
        "org_form",
        "sektor_2014",
        "undersektor_2014",
    )
    paths_post2014 = _get_all_bof_situttak_october_paths(want_cols_post2014)
    paths_pre2014 = [
        p for p in paths_pre2014 if p not in paths_post2014
    ]  # Keep only data with fewer columns if we have to

    union_parts: list[str] = []

    for path in paths_pre2014:
        path_str = str(path).replace("'", "''")
        path_period = _first_date_from_path_period(path).strftime(r"%Y-%m-%d")

        union_parts.append(f"""
            SELECT
                org_nr as orgnr_foretak,
                NULL as orgnrbed,
                NULL as org_form,
                NULL as sektor_2014,
                NULL as undersektor_2014,
                sektor,
                CAST('{path_period}' as DATE) as bof_period_date
            FROM read_parquet('{path_str}')
            """)

    for path in paths_post2014:
        path_str = str(path).replace("'", "''")
        path_period = _first_date_from_path_period(path).strftime(r"%Y-%m-%d")

        union_parts.append(f"""
            SELECT
                org_nr as orgnr_foretak,
                orgnrbed,
                org_form,
                sektor_2014,
                undersektor_2014,
                NULL as sektor,
                CAST('{path_period}' as DATE) as bof_period_date
            FROM read_parquet('{path_str}')
            """)

    if not union_parts:
        logger.warning(
            "Found no BOF files with the columns needed to build bof_eierforhold. Creating an empty view."
        )
        _create_empty_view(
            alias,
            connection,
            {
                "orgnr_foretak": "VARCHAR",
                "orgnrbed": "VARCHAR",
                "bof_period_date": "DATE",
                "bof_eierforhold": "VARCHAR",
            },
        )
        return

    union_sql = UNION_ALL.join(union_parts)

    query = f"""
        CREATE OR REPLACE VIEW {alias} AS
        SELECT
            orgnr_foretak,
            orgnrbed,
            /*  -- only needed for debug
            org_form,
            sektor,
            sektor_2014,
            undersektor_2014,
            */
            bof_period_date,
            CASE
                -- VGU-koding
                WHEN org_form        == 'KIRK' THEN '3'
                WHEN org_form == 'STAT' and sektor_2014 == '6100' THEN '1'  -- Statlig
                WHEN org_form == 'SÆR'  AND sektor_2014 == '6100' AND undersektor_2014 == '005' THEN '1'  -- Statlig
                WHEN org_form == 'KOMM' AND sektor_2014 == '6500' AND undersektor_2014 == '006' THEN '4'  -- Kommune
                WHEN org_form == 'KF'   AND sektor_2014 == '6500' AND undersektor_2014 == '006' THEN '4'  -- Kommune
                WHEN org_form == 'IKS'  AND sektor_2014 == '6500' AND undersektor_2014 == '006' THEN '4'  -- Kommune
                WHEN org_form == 'ORGL' AND sektor_2014 == '6500' AND undersektor_2014 == '006' THEN '4'  -- Kommune
                WHEN org_form == 'FYLK' AND sektor_2014 == '6500' AND undersektor_2014 == '007' THEN '5'  -- Fylkeskommune

                -- Grunnskolekoding - gjelder denne alle årganger? Skummelt?
                WHEN undersektor_2014 == '001' THEN '3' -- Privat
                WHEN undersektor_2014 == '005' THEN '1' -- Statlig
                WHEN undersektor_2014 == '006' THEN '4' -- Kommune
                WHEN undersektor_2014 == '007' THEN '5' -- Fylkeskommune

                -- Andre omkodinger basert på klassifikasjon 39 i klass
                WHEN SUBSTR(sektor_2014,1,1) in ('2', '8') THEN '3' -- Privat
                WHEN SUBSTR(sektor_2014,2,1) in ('11', '61') THEN '1'  -- Statlig
                WHEN SUBSTR(sektor_2014,2,1) in ('15', '65') THEN '4'  -- Kommune

                -- gammel koding på kun sektor (før 2014-variablene fantes)
                WHEN sektor == '510' THEN '5'  -- Fylkeskommune
                WHEN sektor in ('550', '660') THEN '4'  -- Kommune
                WHEN sektor in ('710', '717', '760', '790', '740') THEN '3' -- Privat
                WHEN sektor in ('610', '630', '635', '190', '390', '110') THEN '1'  -- Statlig

                -- Skoler som mangler sektorer har en tendens til å være Private
                -- We will only make this guess if the orgnr has a value in one of these columns - otherwise we dont knwo
                WHEN (sektor_2014 IS DISTINCT FROM NULL OR undersektor_2014 IS DISTINCT FROM NULL OR sektor IS DISTINCT FROM NULL) THEN '3'

                -- Dont make a guess when we lack information that matches
                ELSE NULL
            END AS bof_eierforhold
        FROM ({union_sql})
        WHERE
            orgnr_foretak IS NOT NULL AND TRIM(CAST(orgnr_foretak AS VARCHAR)) != '' AND orgnr_foretak != '000000000'

        ;
    """
    connection.sql(query)


def _generate_bof_unique_orgnrbed_view(
    alias: str,
    connection: db.DuckDBPyConnection,
) -> None:
    """All unique orgnrbed values, keeping only the latest BOF placement per orgnr."""
    latest_placement_ctes_sql = _bof_latest_orgnr_placement_ctes_sql()

    if latest_placement_ctes_sql is None:
        logger.warning(
            "Found no BOF files with orgnrbed to build the unique orgnrbed view. Creating an empty view."
        )
        _create_empty_view(alias, connection, {"orgnrbed": "VARCHAR"})
        return

    query = f"""
        CREATE OR REPLACE VIEW {alias} AS
        WITH {latest_placement_ctes_sql}
        SELECT DISTINCT
            orgnr AS orgnrbed
        FROM latest_placement
        WHERE orgnr_type = 'orgnrbed'
        ;
    """

    connection.sql(query)


def _generate_bof_unique_orgnr_foretak_view(
    alias: str,
    connection: db.DuckDBPyConnection,
) -> None:
    """All unique orgnr_foretak values, keeping only the latest BOF placement per orgnr."""
    latest_placement_ctes_sql = _bof_latest_orgnr_placement_ctes_sql()

    if latest_placement_ctes_sql is None:
        logger.warning(
            "Found no BOF files with orgnr to build the unique orgnr_foretak view. Creating an empty view."
        )
        _create_empty_view(alias, connection, {"orgnr": "VARCHAR"})
        return

    query = f"""
        CREATE OR REPLACE VIEW {alias} AS
        WITH {latest_placement_ctes_sql}
        SELECT DISTINCT orgnr
        FROM latest_placement
        WHERE orgnr_type = 'foretak'
        ;
    """

    connection.sql(query)


def _first_date_from_path_period(path_with_date: str | Path) -> datetime.date:
    possible_date = get_periods_from_path(path_with_date)
    if isinstance(possible_date, Iterable):
        if not possible_date:
            raise TypeError(
                f"Couldn't get expected periods out from path {path_with_date} (periods are empty)."
            )
        return min(possible_date)
    elif isinstance(possible_date, datetime.datetime | datetime.date):
        return datetime.date(
            year=possible_date.year, month=possible_date.month, day=possible_date.day
        )
    raise TypeError(f"Couldn't get expected periods out from path {path_with_date}")


def _bof_dated_orgnr_connections_lookup_sql(
    input_alias: str,
    orgnr_col: str,
    orgnrbed_col: str,
) -> str | None:
    """Return SQL for BOF connections limited to orgnr values in an input table."""
    paths = _get_all_bof_situttak_october_paths(want_cols=("org_nr", "orgnrbed"))
    latest_placement_ctes_sql = _bof_latest_orgnr_placement_ctes_sql(
        relevant_orgnr_cte="relevant_orgnr"
    )
    union_parts: list[str] = []
    for path in paths:
        path_str = str(path).replace("'", "''")
        path_period = _first_date_from_path_period(path).strftime(r"%Y-%m-%d")

        union_parts.append(f"""
            SELECT DISTINCT
                CAST(org_nr AS VARCHAR) AS orgnr,
                CAST(orgnrbed AS VARCHAR) AS orgnrbed,
                CAST('{path_period}' AS DATE) AS bof_period_date
            FROM read_parquet('{path_str}')
            """)

    if not union_parts or latest_placement_ctes_sql is None:
        return None

    union_sql = UNION_ALL.join(union_parts)
    return f"""
        WITH input_foretak AS (
            SELECT DISTINCT
                CAST({orgnr_col} AS VARCHAR) AS orgnr
            FROM {input_alias}
            WHERE
                {orgnr_col} IS NOT NULL AND
                TRIM(CAST({orgnr_col} AS VARCHAR)) != '' AND
                CAST({orgnr_col} AS VARCHAR) != '000000000'
        ),

        input_orgnrbed AS (
            SELECT DISTINCT
                CAST({orgnrbed_col} AS VARCHAR) AS orgnr
            FROM {input_alias}
            WHERE
                {orgnrbed_col} IS NOT NULL AND
                TRIM(CAST({orgnrbed_col} AS VARCHAR)) != '' AND
                CAST({orgnrbed_col} AS VARCHAR) != '000000000'
        ),

        relevant_orgnr AS (
            SELECT orgnr FROM input_foretak
            UNION
            SELECT orgnr FROM input_orgnrbed
        ),

        raw_connections AS (
            SELECT *
            FROM ({union_sql})
            WHERE
                orgnrbed IS NOT NULL AND
                orgnr IS NOT NULL AND TRIM(orgnr) != '' AND orgnr != '000000000' AND
                TRIM(orgnrbed) != '' AND orgnrbed != '000000000' AND
                orgnr IN (SELECT orgnr FROM input_foretak) AND
                orgnrbed IN (SELECT orgnr FROM input_orgnrbed)
        ),

        {latest_placement_ctes_sql}

        SELECT DISTINCT
            conn.orgnr,
            conn.orgnrbed
        FROM raw_connections AS conn
        JOIN latest_placement AS orgnr_class
            ON conn.orgnr = orgnr_class.orgnr
           AND orgnr_class.orgnr_type = 'foretak'
        JOIN latest_placement AS orgnrbed_class
            ON conn.orgnrbed = orgnrbed_class.orgnr
           AND orgnrbed_class.orgnr_type = 'orgnrbed'
        ;
    """


def _bof_orgnrbed_to_foretak_lookup_sql(
    input_alias: str,
    orgnrbed_col: str,
    join_date_col: str,
    row_id_col: str,
) -> str | None:
    """Return SQL mapping input orgnrbed values to dated orgnr_foretak values."""
    paths = _get_all_bof_situttak_october_paths(want_cols=("org_nr", "orgnrbed"))
    latest_placement_ctes_sql = _bof_latest_orgnr_placement_ctes_sql(
        relevant_orgnr_cte="relevant_orgnr"
    )
    union_parts: list[str] = []
    for path in paths:
        path_str = str(path).replace("'", "''")
        path_period = _first_date_from_path_period(path).strftime(r"%Y-%m-%d")

        union_parts.append(f"""
            SELECT DISTINCT
                CAST(org_nr AS VARCHAR) AS orgnr,
                CAST(orgnrbed AS VARCHAR) AS orgnrbed,
                CAST('{path_period}' AS DATE) AS bof_period_date
            FROM read_parquet('{path_str}')
            """)

    if not union_parts or latest_placement_ctes_sql is None:
        return None

    union_sql = UNION_ALL.join(union_parts)
    return f"""
        WITH input_clean AS (
            SELECT
                {row_id_col} AS _row_id,
                TRIM(CAST({orgnrbed_col} AS VARCHAR)) AS orgnrbed,
                CAST({join_date_col} AS DATE) AS join_date
            FROM {input_alias}
            WHERE
                {orgnrbed_col} IS NOT NULL AND
                TRIM(CAST({orgnrbed_col} AS VARCHAR)) != '' AND
                CAST({orgnrbed_col} AS VARCHAR) != '000000000' AND
                {join_date_col} IS NOT NULL
        ),

        input_keys AS (
            SELECT DISTINCT
                orgnrbed,
                join_date
            FROM input_clean
        ),

        relevant_orgnrbed AS (
            SELECT DISTINCT
                orgnrbed
            FROM input_keys
        ),

        raw_connections AS (
            SELECT *
            FROM ({union_sql})
            WHERE
                orgnrbed IS NOT NULL AND
                orgnr IS NOT NULL AND TRIM(orgnr) != '' AND orgnr != '000000000' AND
                TRIM(orgnrbed) != '' AND orgnrbed != '000000000' AND
                orgnrbed IN (SELECT orgnrbed FROM relevant_orgnrbed)
        ),

        relevant_orgnr AS (
            SELECT orgnr FROM raw_connections
            UNION
            SELECT orgnrbed AS orgnr FROM raw_connections
        ),

        {latest_placement_ctes_sql},

        conn_base AS (
            SELECT
                conn.orgnrbed,
                conn.orgnr,
                conn.bof_period_date
            FROM raw_connections AS conn
            JOIN latest_placement AS orgnr_class
                ON conn.orgnr = orgnr_class.orgnr
               AND orgnr_class.orgnr_type = 'foretak'
            JOIN latest_placement AS orgnrbed_class
                ON conn.orgnrbed = orgnrbed_class.orgnr
               AND orgnrbed_class.orgnr_type = 'orgnrbed'
        ),

        conn_changes AS (
            SELECT
                orgnrbed,
                orgnr,
                bof_period_date
            FROM (
                SELECT
                    orgnrbed,
                    orgnr,
                    bof_period_date,
                    LAG(orgnr) OVER (
                        PARTITION BY orgnrbed
                        ORDER BY bof_period_date
                    ) AS prev_orgnr
                FROM conn_base
            )
            WHERE prev_orgnr IS NULL
               OR orgnr IS DISTINCT FROM prev_orgnr
        ),

        resolved_keys AS (
            SELECT
                k.orgnrbed,
                k.join_date,
                COALESCE(
                    (
                        SELECT c.orgnr
                        FROM conn_changes AS c
                        WHERE c.orgnrbed = k.orgnrbed
                          AND c.bof_period_date <= k.join_date
                        ORDER BY c.bof_period_date DESC
                        LIMIT 1
                    ),
                    (
                        SELECT c.orgnr
                        FROM conn_changes AS c
                        WHERE c.orgnrbed = k.orgnrbed
                          AND c.bof_period_date > k.join_date
                        ORDER BY c.bof_period_date ASC
                        LIMIT 1
                    )
                ) AS orgnr
            FROM input_keys AS k
        ),

        resolved AS (
            SELECT
                inp._row_id,
                rk.orgnr
            FROM input_clean AS inp
            LEFT JOIN resolved_keys AS rk
                ON inp.orgnrbed = rk.orgnrbed
               AND inp.join_date = rk.join_date
        )

        SELECT
            raw.{row_id_col} AS _row_id,
            resolved.orgnr
        FROM {input_alias} AS raw
        LEFT JOIN resolved
            ON raw.{row_id_col} = resolved._row_id
        ORDER BY raw.{row_id_col}
        ;
    """


def _bof_foretak_to_orgnrbed_lookup_sql(
    input_alias: str,
    orgnr_col: str,
    join_date_col: str,
    row_id_col: str,
) -> str | None:
    """Return SQL mapping input orgnr_foretak values to dated one-to-one orgnrbed values."""
    paths = _get_all_bof_situttak_october_paths(want_cols=("org_nr", "orgnrbed"))
    latest_placement_ctes_sql = _bof_latest_orgnr_placement_ctes_sql(
        relevant_orgnr_cte="relevant_orgnr"
    )
    union_parts: list[str] = []
    for path in paths:
        path_str = str(path).replace("'", "''")
        path_period = _first_date_from_path_period(path).strftime(r"%Y-%m-%d")

        union_parts.append(f"""
            SELECT DISTINCT
                CAST(org_nr AS VARCHAR) AS orgnr,
                CAST(orgnrbed AS VARCHAR) AS orgnrbed,
                CAST('{path_period}' AS DATE) AS bof_period_date
            FROM read_parquet('{path_str}')
            """)

    if not union_parts or latest_placement_ctes_sql is None:
        return None

    union_sql = UNION_ALL.join(union_parts)
    return f"""
        WITH input_clean AS (
            SELECT
                {row_id_col} AS _row_id,
                TRIM(CAST({orgnr_col} AS VARCHAR)) AS orgnr,
                CAST({join_date_col} AS DATE) AS join_date
            FROM {input_alias}
            WHERE
                {orgnr_col} IS NOT NULL AND
                TRIM(CAST({orgnr_col} AS VARCHAR)) != '' AND
                CAST({orgnr_col} AS VARCHAR) != '000000000' AND
                {join_date_col} IS NOT NULL
        ),

        input_keys AS (
            SELECT DISTINCT
                orgnr,
                join_date
            FROM input_clean
        ),

        relevant_input_orgnr AS (
            SELECT DISTINCT
                orgnr
            FROM input_keys
        ),

        raw_connections AS (
            SELECT *
            FROM ({union_sql})
            WHERE
                orgnrbed IS NOT NULL AND
                orgnr IS NOT NULL AND TRIM(orgnr) != '' AND orgnr != '000000000' AND
                TRIM(orgnrbed) != '' AND orgnrbed != '000000000' AND
                orgnr IN (SELECT orgnr FROM relevant_input_orgnr)
        ),

        relevant_orgnr AS (
            SELECT orgnr FROM raw_connections
            UNION
            SELECT orgnrbed AS orgnr FROM raw_connections
        ),

        {latest_placement_ctes_sql},

        conn_base AS (
            SELECT
                conn.orgnr,
                conn.orgnrbed,
                conn.bof_period_date
            FROM raw_connections AS conn
            JOIN latest_placement AS orgnr_class
                ON conn.orgnr = orgnr_class.orgnr
               AND orgnr_class.orgnr_type = 'foretak'
            JOIN latest_placement AS orgnrbed_class
                ON conn.orgnrbed = orgnrbed_class.orgnr
               AND orgnrbed_class.orgnr_type = 'orgnrbed'
        ),

        conn_periods AS (
            SELECT
                orgnr,
                bof_period_date,
                COUNT(DISTINCT orgnrbed) AS orgnrbed_count,
                MIN(orgnrbed) AS single_orgnrbed
            FROM conn_base
            GROUP BY
                orgnr,
                bof_period_date
        ),

        chosen_periods AS (
            SELECT
                k.orgnr,
                k.join_date,
                COALESCE(
                    (
                        SELECT p.bof_period_date
                        FROM conn_periods AS p
                        WHERE p.orgnr = k.orgnr
                          AND p.bof_period_date <= k.join_date
                        ORDER BY p.bof_period_date DESC
                        LIMIT 1
                    ),
                    (
                        SELECT p.bof_period_date
                        FROM conn_periods AS p
                        WHERE p.orgnr = k.orgnr
                          AND p.bof_period_date > k.join_date
                        ORDER BY p.bof_period_date ASC
                        LIMIT 1
                    )
                ) AS chosen_period
            FROM input_keys AS k
        ),

        resolved_keys AS (
            SELECT
                cp.orgnr,
                cp.join_date,
                CASE
                    WHEN p.orgnrbed_count = 1 THEN p.single_orgnrbed
                    ELSE NULL
                END AS orgnrbed
            FROM chosen_periods AS cp
            LEFT JOIN conn_periods AS p
                ON p.orgnr = cp.orgnr
               AND p.bof_period_date = cp.chosen_period
        ),

        resolved AS (
            SELECT
                inp._row_id,
                rk.orgnrbed
            FROM input_clean AS inp
            LEFT JOIN resolved_keys AS rk
                ON inp.orgnr = rk.orgnr
               AND inp.join_date = rk.join_date
        )

        SELECT
            raw.{row_id_col} AS _row_id,
            resolved.orgnrbed
        FROM {input_alias} AS raw
        LEFT JOIN resolved
            ON raw.{row_id_col} = resolved._row_id
        ORDER BY raw.{row_id_col}
        ;
    """


@lru_cache
def _get_all_bof_situttak_october_paths(
    want_cols: tuple[str, ...] | None = None,
) -> list[Path]:
    shared_folder = Path(
        settings.paths.daplalab_mounted.get("shared_root_external", "/buckets/shared")
    )
    with_bucket = (
        shared_folder
        / settings.datasets.bof_situttak.team
        / settings.datasets.bof_situttak.bucket
    )

    glob_pattern = settings.datasets.bof_situttak.path_glob
    all_bof_monthly = sorted(with_bucket.glob(glob_pattern))
    if not all_bof_monthly:
        logger.warning(
            f"Found no BOF files on disk for glob '{glob_pattern}' under {with_bucket}."
        )
        return []

    if want_cols is None:
        want_cols_list: tuple[str, ...] = ("org_nr", "orgnrbed")
    else:
        want_cols_list = want_cols
    all_bof_monthly_has_want_cols = [
        p
        for p in all_bof_monthly
        if all(c in pyarrow_columns_from_metadata(p) for c in want_cols_list)
    ]
    if not all_bof_monthly_has_want_cols:
        logger.warning(
            f"Found BOF files on disk, but none with the expected columns {want_cols_list}."
        )
        return []

    # If the wanted columns are missing from the last file... We raise a warning as the file might have changed away from our expectations
    last_file_columns = pyarrow_columns_from_metadata(all_bof_monthly[-1])
    if not all(c in last_file_columns for c in want_cols_list):
        logger.warning(
            f"The last bof situttak does not have the columns we expect: {want_cols_list} - this means the nudb_use package needs fixing most likely. {all_bof_monthly[-1]}"
        )

    # If the last file's date is too far from the current year, we should be worried that they have stopped producing the files there
    last_year = datetime.datetime.now().year - 1
    last_file_year = _first_date_from_path_period(all_bof_monthly[-1]).year
    if last_year > last_file_year:
        logger.warning(
            f"The last bof situttak is from the year {last_file_year} - we expect this to be the same or later than last current year: {last_year}, path: {all_bof_monthly[-1]}"
        )

    picked_bof = [
        p
        for p in all_bof_monthly_has_want_cols
        if _first_date_from_path_period(p).month == 10
    ]

    # Add the first and last file if not already picked
    for i in [0, -1]:
        if all_bof_monthly_has_want_cols[i] not in picked_bof:
            picked_bof.append(all_bof_monthly_has_want_cols[i])

    picked_bof = sorted(get_latest_fileversions(picked_bof))
    logger.info(
        f"Picked {picked_bof[0].stem} as first bof-file, and {picked_bof[-1].stem} as the last."
    )
    return picked_bof
