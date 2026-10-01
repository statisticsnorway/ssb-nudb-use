import duckdb as db
from nudb_use.nudb_logger import logger


def _generate_microdata_vgs_fagvurdering_view(
    alias: str, connection: db.DuckDBPyConnection
) -> None:
    """Generer samlet microdata-datasett for videregående skole (vgs) i langformat."""
    from nudb_use.datasets.nudb_data import NudbData
    from nudb_use.datasets.nudb_database import nudb_database

    logger.info("Generating `_microdata_vgs_fagvurdering` dataset view.")

    if (
        "_microdata_videregaaendekarakterer" in nudb_database._dataset_generators
        and "_microdata_videregaaende_karakterer"
        not in nudb_database._dataset_generators
    ):
        vgs_source = NudbData("_microdata_videregaaendekarakterer")
    else:
        vgs_source = NudbData("_microdata_videregaaende_karakterer")

    columns = [
        row[0]
        for row in connection.execute(f"DESCRIBE {vgs_source.alias}").fetchall()
    ]

    fagkode_col = "fagkode" if "fagkode" in columns else "vgs_fagkode"
    stp_col = "stp" if "stp" in columns else "vg_karakter_standpunkt"
    skr_col = "skr" if "skr" in columns else "vg_karakter_skriftlig"
    mun_col = "mun" if "mun" in columns else "vg_karakter_muntlig"
    snr_col = "snr" if "snr" in columns else "fnr"

    parts = []

    if stp_col in columns:
        parts.append(f"""
            SELECT
                CONCAT({snr_col}, '_', {fagkode_col}, '_stp') AS id,
                {fagkode_col} AS fagvurdering_vgs_fagkode,
                {stp_col} AS fagvurdering_vgs_karakter,
                'stp' AS fagvurdering_vgs_vurderingsform,
                CAST(NULL AS VARCHAR) AS fagvurdering_vgs_skole,
                CAST(NULL AS VARCHAR) AS start,
                CAST(NULL AS VARCHAR) AS stop
            FROM
                {vgs_source.alias}
            WHERE
                {stp_col} IS NOT NULL AND {stp_col} != ''
        """)

    if skr_col in columns:
        parts.append(f"""
            {"UNION ALL" if parts else ""}
            SELECT
                CONCAT({snr_col}, '_', {fagkode_col}, '_skr') AS id,
                {fagkode_col} AS fagvurdering_vgs_fagkode,
                {skr_col} AS fagvurdering_vgs_karakter,
                'skr' AS fagvurdering_vgs_vurderingsform,
                CAST(NULL AS VARCHAR) AS fagvurdering_vgs_skole,
                CAST(NULL AS VARCHAR) AS start,
                CAST(NULL AS VARCHAR) AS stop
            FROM
                {vgs_source.alias}
            WHERE
                {skr_col} IS NOT NULL AND {skr_col} != ''
        """)

    if mun_col in columns:
        parts.append(f"""
            {"UNION ALL" if parts else ""}
            SELECT
                CONCAT({snr_col}, '_', {fagkode_col}, '_mun') AS id,
                {fagkode_col} AS fagvurdering_vgs_fagkode,
                {mun_col} AS fagvurdering_vgs_karakter,
                'mun' AS fagvurdering_vgs_vurderingsform,
                CAST(NULL AS VARCHAR) AS fagvurdering_vgs_skole,
                CAST(NULL AS VARCHAR) AS start,
                CAST(NULL AS VARCHAR) AS stop
            FROM
                {vgs_source.alias}
            WHERE
                {mun_col} IS NOT NULL AND {mun_col} != ''
        """)

    if not parts:
        connection.execute(
            f"""
            CREATE OR REPLACE VIEW {alias} AS
            SELECT
                CAST(NULL AS VARCHAR) AS id,
                CAST(NULL AS VARCHAR) AS fagvurdering_vgs_fagkode,
                CAST(NULL AS VARCHAR) AS fagvurdering_vgs_karakter,
                CAST(NULL AS VARCHAR) AS fagvurdering_vgs_vurderingsform,
                CAST(NULL AS VARCHAR) AS fagvurdering_vgs_skole,
                CAST(NULL AS VARCHAR) AS start,
                CAST(NULL AS VARCHAR) AS stop
            WHERE FALSE
            """
        )
    else:
        union_query = "\n".join(parts)
        connection.execute(f"CREATE OR REPLACE VIEW {alias} AS {union_query}")
