import duckdb as db

from nudb_use.nudb_logger import logger


def _generate_microdata_nasjprov_view(
    alias: str,
    connection: db.DuckDBPyConnection,
) -> None:
    """Generate pivoted view of nasjprov for Microdata."""
    from nudb_use.datasets import NudbData  # Avoids circular import

    logger.info("Deriving '_microdata_nasjprov' pivoted dataset.")

    nasjprov = NudbData("nasjprov")

    query = f"""
        CREATE OR REPLACE VIEW {alias} AS (
            SELECT
                snr,
                utd_skoleaar_start AS np_utd_skoleaar_start,
                orgnrbed AS np_orgnrbed,
                orgnr_foretak AS np_orgnr_foretak,
                avgiverskole_orgnrbed AS np_avgiverskole_orgnrbed,
                avgiverskole_orgnr_foretak AS np_avgiverskole_orgnr_foretak,
                utd_skolekom AS np_utd_skolekom,
                deltattstatus AS np_deltattstatus,

                -- Conditional aggregation (SQL Pivot) for NPENG05
                MAX(CASE WHEN provekode = 'NPENG05' THEN skalapoeng END) AS np_skalapoeng_npeng05,
                MAX(CASE WHEN provekode = 'NPENG05' THEN mestringsnivaa END) AS np_mestringsnivaa_npeng05
            FROM
                {nasjprov.alias}
            GROUP BY
                snr,
                utd_skoleaar_start,
                orgnrbed,
                orgnr_foretak,
                avgiverskole_orgnrbed,
                avgiverskole_orgnr_foretak,
                utd_skolekom,
                deltattstatus
        );
    """

    connection.execute(query)
