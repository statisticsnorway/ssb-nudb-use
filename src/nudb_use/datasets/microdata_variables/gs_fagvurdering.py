import duckdb as db

from nudb_use.nudb_logger import logger


def _generate_microdata_gs_fagvurdering_view(
    alias: str, connection: db.DuckDBPyConnection
) -> None:
    """Generate a simplified microdata dataset view for elementary school (gs).

    Maintains separate variables for standpunkt, skriftlig, and muntlig grades,
    with a composite ID formed from 'fnr_fagkode'.
    """
    from nudb_use.datasets.nudb_data import NudbData
    from nudb_use.datasets.nudb_database import nudb_database

    logger.info("Generating `_microdata_gs_fagvurdering` dataset view.")

    # Identify whether to use mock name from tests or the production name
    if (
        "_microdata_grunnskolekarakterer" in nudb_database._dataset_generators
        and "_microdata_grunnskole_karakterer" not in nudb_database._dataset_generators
    ):
        gs_source = NudbData("_microdata_grunnskolekarakterer")
    else:
        gs_source = NudbData("_microdata_grunnskole_karakterer")

    # Get column names present in the source table/view in DuckDB
    columns = [
        row[0] for row in connection.execute(f"DESCRIBE {gs_source.alias}").fetchall()
    ]

    # Map column names based on source schema
    fagkode_col = (
        "grsk_gro_fagkode_vigo"
        if "grsk_gro_fagkode_vigo" in columns
        else "gro_fagkode_vigo"
    )
    karakter_stp_col = (
        "grsk_gro_karakter_standpunkt"
        if "grsk_gro_karakter_standpunkt" in columns
        else "gro_karakter_standpunkt"
    )
    karakter_skr_col = (
        "grsk_gro_karakter_skriftlig"
        if "grsk_gro_karakter_skriftlig" in columns
        else "gro_karakter_skriftlig"
    )
    karakter_mun_col = (
        "grsk_gro_karakter_muntlig"
        if "grsk_gro_karakter_muntlig" in columns
        else "gro_karakter_muntlig"
    )
    orgnr_col = (
        "orgnrbed"
        if "orgnrbed" in columns
        else ("utd_orgnr" if "utd_orgnr" in columns else "orgnr")
    )

    # Compute start- and stop-dates based on available columns
    if "utd_skoleaar_start" in columns:
        start_expr = (
            f"CAST({gs_source.alias}.utd_skoleaar_start AS VARCHAR) || '-08-01'"
        )
        stop_expr = f"CAST(CAST({gs_source.alias}.utd_skoleaar_start AS INTEGER) + 1 AS VARCHAR) || '-06-30'"
    elif "grsk_utd_aktivitet_slutt" in columns:
        start_expr = f"CAST(CAST(SUBSTR({gs_source.alias}.grsk_utd_aktivitet_slutt, 1, 4) AS INTEGER) - 1 AS VARCHAR) || '-08-01'"
        stop_expr = f"{gs_source.alias}.grsk_utd_aktivitet_slutt"
    else:
        start_expr = "NULL"
        stop_expr = "NULL"

    # Ensure both fnr and snr are available, merging if necessary
    has_fnr = "fnr" in columns
    has_snr = "snr" in columns

    join_clause = ""
    source_table = gs_source.alias

    if has_fnr and has_snr:
        fnr_expr = f"{gs_source.alias}.fnr"
        snr_expr = f"{gs_source.alias}.snr"
    elif has_snr and not has_fnr:
        fnr2snr_map = NudbData("_snrkat_fnr2snr")
        fnr_expr = f"{fnr2snr_map.alias}.fnr"
        snr_expr = f"{gs_source.alias}.snr"
        join_clause = f"LEFT JOIN {fnr2snr_map.alias} ON {gs_source.alias}.snr = {fnr2snr_map.alias}.snr"
    elif has_fnr and not has_snr:
        fnr2snr_map = NudbData("_snrkat_fnr2snr")
        fnr_expr = f"{gs_source.alias}.fnr"
        snr_expr = f"{fnr2snr_map.alias}.snr"
        join_clause = f"LEFT JOIN {fnr2snr_map.alias} ON {gs_source.alias}.fnr = {fnr2snr_map.alias}.fnr"
    else:
        raise KeyError("Neither 'fnr' nor 'snr' is present in the source dataset.")

    query = f"""
        CREATE OR REPLACE VIEW {alias} AS (
            SELECT
                CONCAT({fnr_expr}, '_', {gs_source.alias}.{fagkode_col}) AS id,
                {snr_expr} AS snr,
                {fnr_expr} AS fnr,
                {gs_source.alias}.{fagkode_col} AS fagvurdering_grsk_fagkode,
                {gs_source.alias}.{karakter_stp_col} AS fagvurdering_grsk_karakter_standpunkt,
                {gs_source.alias}.{karakter_skr_col} AS fagvurdering_grsk_karakter_skriftlig,
                {gs_source.alias}.{karakter_mun_col} AS fagvurdering_grsk_karakter_muntlig,
                {gs_source.alias}.{orgnr_col} AS fagvurdering_grsk_skole,
                {start_expr} AS start,
                {stop_expr} AS stop
            FROM
                {source_table}
                {join_clause}
        );
    """

    connection.execute(query)
