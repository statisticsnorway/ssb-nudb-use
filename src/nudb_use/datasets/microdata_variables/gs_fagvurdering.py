import duckdb as db
from nudb_use.nudb_logger import logger


def _generate_microdata_gs_fagvurdering_view(
    alias: str, connection: db.DuckDBPyConnection
) -> None:
    """Generate a simplified microdata dataset view for elementary school (gs).

    Maintains separate variables for standpunkt, skriftlig, and muntlig grades,
    with a composite ID formed from 'snr_fagkode'.
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
        row[0]
        for row in connection.execute(f"DESCRIBE {gs_source.alias}").fetchall()
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
        "grsk_utd_skolekom"
        if "grsk_utd_skolekom" in columns
        else ("utd_orgnr" if "utd_orgnr" in columns else "orgnr")
    )

    # Compute start- and stop-dates based on available columns
    if "utd_skoleaar_start" in columns:
        start_expr = "CAST(utd_skoleaar_start AS VARCHAR) || '-08-01'"
        stop_expr = (
            "CAST(CAST(utd_skoleaar_start AS INTEGER) + 1 AS VARCHAR) || '-06-30'"
        )
    elif "grsk_utd_aktivitet_slutt" in columns:
        start_expr = "CAST(CAST(SUBSTR(grsk_utd_aktivitet_slutt, 1, 4) AS INTEGER) - 1 AS VARCHAR) || '-08-01'"
        stop_expr = "grsk_utd_aktivitet_slutt"
    else:
        start_expr = "NULL"
        stop_expr = "NULL"

    snr_col = "snr" if "snr" in columns else "fnr"

    query = f"""
        CREATE OR REPLACE VIEW {alias} AS (
            SELECT
                CONCAT({snr_col}, '_', {fagkode_col}) AS id,
                {snr_col} AS snr,
                {fagkode_col} AS fagvurdering_grsk_fagkode,
                {karakter_stp_col} AS fagvurdering_grsk_karakter_standpunkt,
                {karakter_skr_col} AS fagvurdering_grsk_karakter_skriftlig,
                {karakter_mun_col} AS fagvurdering_grsk_karakter_muntlig,
                {orgnr_col} AS fagvurdering_grsk_skole,
                {start_expr} AS start,
                {stop_expr} AS stop
            FROM
                {gs_source.alias}
        );
    """

    connection.execute(query)
