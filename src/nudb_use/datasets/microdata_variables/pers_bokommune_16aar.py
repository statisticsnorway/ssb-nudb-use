import duckdb as db
import pandas as pd
from nudb_use.nudb_logger import logger

def _generate_microdata_pers_bokommune_16aar_view(
    alias: str,
    connection: db.DuckDBPyConnection,
) -> None:
    """Generate the `pers_bokommune_16aar` Microdata dataset view.

    Exposes a person's municipality of residence at age 16 as `komm_16`.
    """
    from nudb_use.datasets import NudbData
    from nudb_use.datasets.nudb_database import STRING_DTYPE
    from nudb_use.variables import derive

    logger.info("Generating `_microdata_pers_bokommune_16aar` dataset view.")

    cohort = NudbData("utd_person")
    df = cohort.select("snr").df()
    df["snr"] = df["snr"].astype(STRING_DTYPE)

    df = derive.pers_bokommune_16aar(df)

    if "pers_bokommune_16aar" in df.columns:
        df["komm_16"] = df["pers_bokommune_16aar"]
    else:
        df["komm_16"] = pd.Series([pd.NA] * len(df), dtype="string")

    df = df[["snr", "komm_16"]]

    connection.register("_temp_pers_bokommune_16aar_df", df)
    connection.execute(
        f"CREATE OR REPLACE VIEW {alias} AS SELECT * FROM _temp_pers_bokommune_16aar_df"
    )
