import duckdb as db
import datetime
from pathlib import Path
from nudb_use.paths.path_parse import get_periods_from_path
from fagfunksjoner.paths.versions import get_latest_fileversions
from nudb_use.variables.checks import pyarrow_columns_from_metadata
from nudb_config import settings
from nudb_use.nudb_logger import logger

def _generate_vof_unique_orgnrbed(
    alias: str,
    connection: db.DuckDBPyConnection,
) -> None:
    """All the unique orgnrbed from october-files and the first and last files with orgnrbed in."""
    paths = _get_all_vof_situttak_october_paths()
    union_parts: list[str] = []
    for path in paths:
        path_str = str(path).replace("'", "''")
        union_parts.append(
            f"""
            SELECT DISTINCT
                orgnrbed
            FROM read_parquet('{path_str}')
            """
        )

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
        union_parts.append(
            f"""
            SELECT DISTINCT
                org_nr as orgnr
            FROM read_parquet('{path_str}')
            """
        )

    union_sql = "\nUNION ALL\n".join(union_parts)

    query = f"""
        CREATE OR REPLACE VIEW {alias} AS
        SELECT DISTINCT orgnr
        FROM ({union_sql})
        WHERE orgnr IS NOT NULL AND TRIM(CAST(orgnr AS VARCHAR)) != ''
        ;
    """

    connection.sql(query)

def _generate_vof_dated_orgnr_connections(
    alias: str,
    connection: db.DuckDBPyConnection,
) -> None:
    """All the unique orgnr <-> orgnrbed connections from october-files and the first and last files with orgnrbed in."""
    paths = _get_all_vof_situttak_october_paths()
    union_parts: list[str] = []
    for path in paths:
        path_str = str(path).replace("'", "''")
        path_period = get_periods_from_path(path)[0].strftime(r"%Y-%m-%d")
        union_parts.append(
            f"""
            SELECT DISTINCT
                org_nr as orgnr,
                orgnrbed
                CAST('{path_period}' as DATE) as vof_period_date
            FROM read_parquet('{path_str}')
            """
        )

    union_sql = "\nUNION ALL\n".join(union_parts)

    query = f"""
        CREATE OR REPLACE VIEW {alias} AS
        FROM ({union_sql})
        WHERE 
            orgnr IS NOT NULL AND TRIM(CAST(orgnr AS VARCHAR)) != '' AND
            orgnrbed IS NOT NULL AND TRIM(CAST(orgnrbed AS VARCHAR)) != ''
        ;
    """

    connection.sql(query)


def _get_all_vof_situttak_october_paths() -> list[Path]:
    shared_folder = Path(settings.paths.daplalab_mounted.shared_root_external)
    with_bucket = shared_folder / settings.datasets.vof_situttak.team / settings.datasets.vof_situttak.bucket
    
    glob_pattern = settings.datasets.vof_situttak.path_glob
    all_vof_monthly = sorted(with_bucket.glob(glob_pattern))
    want_cols = ["org_nr", "orgnrbed"]
    all_vof_monthly_has_want_cols = [p for p in all_vof_monthly if all([c in pyarrow_columns_from_metadata(p) for c in want_cols])]
    
    # If the wanted columns are missing from the last file... We raise a warning as the file might have changed away from our expectations
    if not all([c in pyarrow_columns_from_metadata(all_vof_monthly[-1]) for c in want_cols]):
        logger.warning(f"The last vof situttak does not have the columns we expect: {want_cols} - this means the nudb_use package needs fixing most likely. {all_vof_monthly[-1]}")

    # If the last file's date is too far from the current year, we should be worried that they have stopped producing the files there
    last_year = datetime.datetime.now().year - 1
    last_file_year = get_periods_from_path(all_vof_monthly[-1])[0].year
    if last_year > last_file_year:
        logger.warning(f"The last vof situttak is from the year {last_file_year} - we expect this to be the same or later than last current year: {last_year}, path: {all_vof_monthly[-1]}")

    picked_vof = [p for p in all_vof_monthly_has_want_cols if get_periods_from_path(p)[0].month == 10]
    
    # Add the first and last file if not already picked
    for i in [0, -1]:
        if all_vof_monthly_has_want_cols[i] not in picked_vof:
            picked_vof.append(all_vof_monthly_has_want_cols[i])
    
    picked_vof = sorted(get_latest_fileversions(picked_vof))
    return picked_vof