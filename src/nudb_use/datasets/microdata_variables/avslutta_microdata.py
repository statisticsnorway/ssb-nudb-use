import duckdb as db

from nudb_use.nudb_logger import logger


def _generate_microdata_avslutta_subset_view(
    alias: str,
    connection: db.DuckDBPyConnection,
) -> None:
    """Generate subset view of avslutta for Microdata."""
    from nudb_use.datasets import NudbData  # Avoids circular import

    logger.info("Deriving 'avslutta_subset' Microdata variable")

    avslutta = NudbData("avslutta")
    query = f"""
        CREATE OR REPLACE VIEW  {alias} AS (
            SELECT
                snr, nus2000, utd_aktivitet_start, utd_aktivitet_slutt, utd_skoleaar_start
            FROM
                {avslutta.alias}
            );
    """

    connection.execute(query)
