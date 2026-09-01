import duckdb as db

from nudb_use.nudb_logger import logger


def _generate_microdata_utd_foreldres_utdnivaa_16aar_view(
    alias: str,
    connection: db.DuckDBPyConnection,
) -> None:
    from nudb_use.datasets import NudbData  # Avoids circular import
    from nudb_use.variables.derive.utd_foreldres_utdnivaa import (
        utd_foreldres_utdnivaa_16aar,
    )

    utd_foreldres_utdnivaa_16aar_view = NudbData("utd_foreldres_utdnivaa")
    logger.info("Deriving `utd_foreldres_utdnivaa_16aar` Microdata variable.")

    df = connection.sql(f"""
        SELECT
            snr,
            utd_foreldres_utdnivaa_16aar_nus2000,
            utd_hoeyeste_far_nus2000,
            utd_hoeyeste_mor_nus2000
        FROM
            {utd_foreldres_utdnivaa_16aar_view.alias}
    """).df()

    df = utd_foreldres_utdnivaa_16aar(df)

    connection.register("_TEMP_UTD_FORELDRES_UTDNIVAA_DF", df)

    connection.execute(f"""
        CREATE OR REPLACE VIEW {alias} AS (
            SELECT * FROM _TEMP_UTD_FORELDRES_UTDNIVAA_DF
        );
    """)