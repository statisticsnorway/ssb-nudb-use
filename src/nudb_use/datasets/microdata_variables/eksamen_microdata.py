import duckdb as db

from nudb_use.nudb_logger import logger


def _generate_microdata_eksamen_subset_view(
    alias: str,
    connection: db.DuckDBPyConnection,
) -> None:
    """Generate subset view of eksamen for Microdata."""
    from nudb_use.datasets import NudbData  # Avoids circular import

    logger.info("Deriving 'eksamen_microdata' Microdata variables")


    # Burde defineres bedre i f.eks nudb-config en her
    eksamen_microdata = NudbData("eksamen")
    query = f"""
        CREATE OR REPLACE VIEW  {alias} AS (
            SELECT
                fnr, snr, nus2000, utd_skoleaar_start,
                utd_skolekom, utd_utdanningstype, utd_viderutd_nettbasert,
                uh_eksamen_studpoeng, uh_eksamen_ergjentak,
                uh_eksamen_dato, uh_ereksamenrett, uh_studieprogram
                fuh_nett_eller_stedbasert, 

            FROM
                {eksamen_microdata.alias}
            );
    """

    connection.execute(query)
