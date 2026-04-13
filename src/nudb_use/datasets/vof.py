import datetime
from functools import lru_cache
from pathlib import Path

import duckdb as db
from fagfunksjoner.paths.versions import get_latest_fileversions
from nudb_config import settings

from nudb_use.nudb_logger import logger
from nudb_use.paths.path_parse import get_periods_from_path
from nudb_use.variables.checks import pyarrow_columns_from_metadata


def _generate_vof_unique_orgnrbed(
    alias: str,
    connection: db.DuckDBPyConnection,
) -> None:
    """All the unique orgnrbed from october-files and the first and last files with orgnrbed in."""
    paths = _get_all_vof_situttak_october_paths()
    union_parts: list[str] = []
    for path in paths:
        path_str = str(path).replace("'", "''")
        union_parts.append(f"""
            SELECT DISTINCT
                orgnrbed
            FROM read_parquet('{path_str}')
            """)

    union_sql = "\nUNION ALL\n".join(union_parts)

    query = f"""
        CREATE OR REPLACE VIEW {alias} AS
        SELECT DISTINCT orgnrbed
        FROM ({union_sql})
        WHERE orgnrbed IS NOT NULL AND TRIM(CAST(orgnrbed AS VARCHAR)) != ''
        ;
    """

    connection.sql(query)


def _generate_vof_unique_orgnr_foretak(
    alias: str,
    connection: db.DuckDBPyConnection,
) -> None:
    """All the unique orgnrbed from october-files and the first and last files with orgnrbed in."""
    paths = _get_all_vof_situttak_october_paths()
    union_parts: list[str] = []
    for path in paths:
        path_str = str(path).replace("'", "''")
        union_parts.append(f"""
            SELECT DISTINCT
                org_nr as orgnr
            FROM read_parquet('{path_str}')
            """)

    union_sql = "\nUNION ALL\n".join(union_parts)

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


def _generate_vof_dated_orgnr_connections(
    alias: str,
    connection: db.DuckDBPyConnection,
) -> None:
    """All the unique orgnr <-> orgnrbed connections from october-files and the first and last files with orgnrbed in."""
    paths = _get_all_vof_situttak_october_paths()
    union_parts: list[str] = []
    for path in paths:
        path_str = str(path).replace("'", "''")
        path_period = _date_from_path_period(path).strftime(r"%Y-%m-%d")

        union_parts.append(f"""
            SELECT DISTINCT
                org_nr as orgnr,
                orgnrbed,
                CAST('{path_period}' as DATE) as vof_period_date
            FROM read_parquet('{path_str}')
            """)

    union_sql = "\nUNION ALL\n".join(union_parts)

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
def _get_all_vof_situttak_october_paths(want_cols: list[str] | None = None) -> list[Path]:
    shared_folder = Path(
        settings.paths.daplalab_mounted.get("shared_root_external", "/buckets/shared")
    )
    with_bucket = (
        shared_folder
        / settings.datasets.vof_situttak.team
        / settings.datasets.vof_situttak.bucket
    )

    glob_pattern = settings.datasets.vof_situttak.path_glob
    all_vof_monthly = sorted(with_bucket.glob(glob_pattern))
    if want_cols is None:
        want_cols_list = ["org_nr", "orgnrbed"]
    else:
        want_cols_list = want_cols
    all_vof_monthly_has_want_cols = [
        p
        for p in all_vof_monthly
        if all([c in pyarrow_columns_from_metadata(p) for c in want_cols_list])
    ]

    # If the wanted columns are missing from the last file... We raise a warning as the file might have changed away from our expectations
    if not all(
        [c in pyarrow_columns_from_metadata(all_vof_monthly[-1]) for c in want_cols_list]
    ):
        logger.warning(
            f"The last vof situttak does not have the columns we expect: {want_cols_list} - this means the nudb_use package needs fixing most likely. {all_vof_monthly[-1]}"
        )

    # If the last file's date is too far from the current year, we should be worried that they have stopped producing the files there
    last_year = datetime.datetime.now().year - 1
    last_file_year = _date_from_path_period(all_vof_monthly[-1]).year
    if last_year > last_file_year:
        logger.warning(
            f"The last vof situttak is from the year {last_file_year} - we expect this to be the same or later than last current year: {last_year}, path: {all_vof_monthly[-1]}"
        )

    picked_vof = [
        p
        for p in all_vof_monthly_has_want_cols
        if _date_from_path_period(p).month == 10
    ]

    # Add the first and last file if not already picked
    for i in [0, -1]:
        if all_vof_monthly_has_want_cols[i] not in picked_vof:
            picked_vof.append(all_vof_monthly_has_want_cols[i])

    picked_vof = sorted(get_latest_fileversions(picked_vof))
    return picked_vof
import duckdb as db


def _generate_vof_eierforhold_view(
    alias: str,
    connection: db.DuckDBPyConnection,
) -> None:
    from nudb_use.datasets.nudb_data import NudbData

    vof_situttak = NudbData("vof_situttak")

    query = f"""
        -- KIRK i orgform skal ha eierforhold privat. undersektor med fra 2014 for å skille ut komm fylk --
        CREATE VIEW
            {alias} AS

        SELECT DISTINCT
            org_nr AS vof_orgnr_foretak,
            orgnrbed AS bof_orgnrbed,
            org_form,

            CASE
                WHEN undersektor_2014 == '001' THEN '3'
                WHEN undersektor_2014 == '005' THEN '1'
                WHEN undersektor_2014 == '006' THEN '4'
                WHEN undersektor_2014 == '007' THEN '5'
                ELSE                                '3'
            END AS vof_eierforhold

        FROM
            {vof_situttak.alias}

        WHERE
            vof_orgnr_foretak IS NOT NULL AND
            orgnrbed          IS NULL AND
            org_form          IS NOT NULL
        ;
    """

    # Logikk i grunnskoledata
    #        undersektor_col = 'undersektor_2014'
    #
    #    # Definer betingelser og tilhørende verdier
    #    conditions = [
    #        (df[undersektor_col] == "006"),
    #        (df[undersektor_col] == "007"),
    #        (df[undersektor_col] == "001"),
    #        (df[undersektor_col] == "005"),
    #        (df[undersektor_col].isna())
    #    ]
    #
    #    values = ["4", "5", "3", "1", "3"]

    connection.sql(query)


def _generate_vof_orgnr_bed2foretak_view(
    alias: str,
    connection: db.DuckDBPyConnection,
) -> None:
    from nudb_use.datasets.nudb_data import NudbData

    vof_situttak = NudbData("vof_situttak")

    query = f"""
        -- KIRK i orgform skal ha eierforhold privat. undersektor med fra 2014 for å skille ut komm fylk --
        CREATE VIEW
            {alias} AS
        SELECT DISTINCT
            orgnrbed AS bof_orgnrbed,
            org_nr   AS vof_orgnr_foretak
        FROM
            {vof_situttak.alias}
        WHERE
            bof_orgnrbed IS NOT NULL AND
            vof_orgnr_foretak IS NOT NULL
        ;
    """

    connection.sql(query)
