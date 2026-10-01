import duckdb as db
from nudb_use.nudb_logger import logger


def _generate_microdata_nasjprov_fagvurdering_view(
    alias: str, connection: db.DuckDBPyConnection
) -> None:
    """Generate the microdata view for national tests (nasjprov) in long format."""
    from nudb_use.datasets import NudbData

    logger.info("Generating `_microdata_nasjprov_fagvurdering` view directly.")

    nasjprov = NudbData("nasjprov")

    columns = [
        row[0]
        for row in connection.execute(f"DESCRIBE {nasjprov.alias}").fetchall()
    ]

    fagkode_col = "provekode" if "provekode" in columns else "fagkode"
    karakter_col = "skalapoeng" if "skalapoeng" in columns else "karakter"
    orgnr_col = "orgnrbed" if "orgnrbed" in columns else "orgnr"
    snr_col = "snr" if "snr" in columns else "fnr"

    query = f"""
        CREATE OR REPLACE VIEW {alias} AS (
            SELECT
                CONCAT({snr_col}, '_', {fagkode_col}, '_NASJONAL_PROVE') AS id,
                {fagkode_col} AS fagvurdering_nasjprov_fagkode,
                {karakter_col} AS fagvurdering_nasjprov_karakter,
                {orgnr_col} AS fagvurdering_nasjprov_skole,
                'NASJONAL_PROVE' AS fagvurdering_nasjprov_vurderingsform,
                CAST(utd_skoleaar_start AS VARCHAR) || '-09-15' AS start,
                CAST(utd_skoleaar_start AS VARCHAR) || '-09-15' AS stop
            FROM
                {nasjprov.alias}
            WHERE
                {karakter_col} IS NOT NULL AND {karakter_col} != ''
        );
    """
    connection.execute(query)
