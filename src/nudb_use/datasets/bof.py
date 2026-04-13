import datetime
from functools import lru_cache
from pathlib import Path

import duckdb as db
from fagfunksjoner.paths.versions import get_latest_fileversions
from nudb_config import settings

from nudb_use.nudb_logger import logger
from nudb_use.paths.path_parse import get_periods_from_path
from nudb_use.variables.checks import pyarrow_columns_from_metadata

UNION_ALL = "\nUNION ALL\n"


def _generate_bof_eierforhold_view(
    alias: str,
    connection: db.DuckDBPyConnection,
) -> None:
    want_cols = ("org_nr", "orgnrbed", "org_form", "sektor_2014", "undersektor_2014")
    paths = _get_all_bof_situttak_october_paths(want_cols)
    union_parts: list[str] = []

    for path in paths:
        if not all([c in pyarrow_columns_from_metadata(path) for c in want_cols]):
            logger.debug(f"Did not find all cols we wanted ({want_cols}) in {path}")
            continue
        path_str = str(path).replace("'", "''")
        path_period = _date_from_path_period(path).strftime(r"%Y-%m-%d")

        union_parts.append(f"""
            SELECT
                org_nr as orgnr_foretak,
                orgnrbed,
                org_form,
                sektor_2014,
                undersektor_2014,
                CAST('{path_period}' as DATE) as bof_period_date
            FROM read_parquet('{path_str}')
            """)

    union_sql = UNION_ALL.join(union_parts)

    query = f"""
        CREATE OR REPLACE VIEW {alias} AS
        SELECT
            orgnr_foretak,
            orgnrbed,
            /*  -- only needed for debug
            org_form,
            sektor_2014,
            undersektor_2014,
            */
            bof_period_date,
            CASE
                -- VGU-koding
                WHEN org_form        == 'KIRK' THEN '3'
                WHEN org_form == 'STAT' and sektor_2014 == '6100' THEN '1'
                WHEN org_form == 'SÆR'  AND sektor_2014 == '6100' AND undersektor_2014 == '005' THEN '1'
                WHEN org_form == 'KOMM' AND sektor_2014 == '6500' AND undersektor_2014 == '006' THEN '4'
                WHEN org_form == 'KF'   AND sektor_2014 == '6500' AND undersektor_2014 == '006' THEN '4'
                WHEN org_form == 'IKS'  AND sektor_2014 == '6500' AND undersektor_2014 == '006' THEN '4'
                WHEN org_form == 'ORGL' AND sektor_2014 == '6500' AND undersektor_2014 == '006' THEN '4'
                WHEN org_form == 'FYLK' AND sektor_2014 == '6500' AND undersektor_2014 == '007' THEN '5'
                -- Grunnskolekoding
                WHEN undersektor_2014 == '001' THEN '3'
                WHEN undersektor_2014 == '005' THEN '1'
                WHEN undersektor_2014 == '006' THEN '4'
                WHEN undersektor_2014 == '007' THEN '5'
                ELSE                                '3'  -- Skoler som mangler sektorer har en tendens til å være Private
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
    """All the unique orgnrbed from october-files and the first and last files with orgnrbed in."""
    paths = _get_all_bof_situttak_october_paths()
    union_parts: list[str] = []
    for path in paths:
        path_str = str(path).replace("'", "''")
        union_parts.append(f"""
            SELECT DISTINCT
                orgnrbed
            FROM read_parquet('{path_str}')
            """)

    union_sql = UNION_ALL.join(union_parts)

    query = f"""
        CREATE OR REPLACE VIEW {alias} AS
        SELECT DISTINCT orgnrbed
        FROM ({union_sql})
        WHERE orgnrbed IS NOT NULL AND TRIM(CAST(orgnrbed AS VARCHAR)) != ''
        ;
    """

    connection.sql(query)


def _generate_bof_unique_orgnr_foretak_view(
    alias: str,
    connection: db.DuckDBPyConnection,
) -> None:
    """All the unique orgnrbed from october-files and the first and last files with orgnrbed in."""
    paths = _get_all_bof_situttak_october_paths()
    union_parts: list[str] = []
    for path in paths:
        path_str = str(path).replace("'", "''")
        union_parts.append(f"""
            SELECT DISTINCT
                org_nr as orgnr
            FROM read_parquet('{path_str}')
            """)

    union_sql = UNION_ALL.join(union_parts)

    query = f"""
        CREATE OR REPLACE VIEW {alias} AS
        SELECT DISTINCT orgnr
        FROM ({union_sql})
        WHERE orgnr IS NOT NULL AND TRIM(CAST(orgnr AS VARCHAR)) != ''
        ;
    """

    connection.sql(query)


def _date_from_path_period(path_with_date: str | Path) -> datetime.date:
    possible_date = get_periods_from_path(path_with_date)
    if isinstance(possible_date, tuple | list | set):
        result = possible_date[0]
    elif isinstance(possible_date, datetime.date | datetime.datetime):
        result = possible_date
    else:
        raise TypeError(f"Couldn't get expected periods out from path {path_with_date}")
    return result


def _generate_bof_dated_orgnr_connections_view(
    alias: str,
    connection: db.DuckDBPyConnection,
) -> None:
    """All the unique orgnr <-> orgnrbed connections from october-files and the first and last files with orgnrbed in."""
    paths = _get_all_bof_situttak_october_paths(want_cols=("org_nr", "orgnrbed"))
    union_parts: list[str] = []
    for path in paths:
        path_str = str(path).replace("'", "''")
        path_period = _date_from_path_period(path).strftime(r"%Y-%m-%d")

        union_parts.append(f"""
            SELECT DISTINCT
                org_nr as orgnr,
                orgnrbed,
                CAST('{path_period}' as DATE) as bof_period_date
            FROM read_parquet('{path_str}')
            """)

    union_sql = UNION_ALL.join(union_parts)

    query = f"""
        CREATE OR REPLACE VIEW {alias} AS
        SELECT *
        FROM ({union_sql})
        WHERE
            orgnrbed IS NOT NULL AND  -- Removes many rows, so most efficient to have first?
            orgnr IS NOT NULL AND TRIM(CAST(orgnr AS VARCHAR)) != '' AND orgnr != '000000000' AND
            TRIM(CAST(orgnrbed AS VARCHAR)) != '' AND orgnrbed != '000000000'
        ;
    """

    connection.sql(query)


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
    if want_cols is None:
        want_cols_list: tuple[str, ...] = ("org_nr", "orgnrbed")
    else:
        want_cols_list = want_cols
    all_bof_monthly_has_want_cols = [
        p
        for p in all_bof_monthly
        if all([c in pyarrow_columns_from_metadata(p) for c in want_cols_list])
    ]

    # If the wanted columns are missing from the last file... We raise a warning as the file might have changed away from our expectations
    if not all(
        [
            c in pyarrow_columns_from_metadata(all_bof_monthly[-1])
            for c in want_cols_list
        ]
    ):
        logger.warning(
            f"The last bof situttak does not have the columns we expect: {want_cols_list} - this means the nudb_use package needs fixing most likely. {all_bof_monthly[-1]}"
        )

    # If the last file's date is too far from the current year, we should be worried that they have stopped producing the files there
    last_year = datetime.datetime.now().year - 1
    last_file_year = _date_from_path_period(all_bof_monthly[-1]).year
    if last_year > last_file_year:
        logger.warning(
            f"The last bof situttak is from the year {last_file_year} - we expect this to be the same or later than last current year: {last_year}, path: {all_bof_monthly[-1]}"
        )

    picked_bof = [
        p
        for p in all_bof_monthly_has_want_cols
        if _date_from_path_period(p).month == 10
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
