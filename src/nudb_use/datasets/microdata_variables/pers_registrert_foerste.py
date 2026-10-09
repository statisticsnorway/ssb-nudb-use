import duckdb as db
import pandas as pd

from nudb_use.nudb_logger import logger

# Mapper fullføringsår til datokolonne.
_YEAR_COLUMN_MAP: dict[str, str] = {
    "aar_foerste_reg_gr": "gr_foerste_registrert_dato",
    "aar_vg_foerste_registrert_dato": "vg_foerste_registrert_dato",
    "aar_vg_foerste_registrert_erutdprogram_dato": "vg_foerste_registrert_erutdprogram_dato",
    "aar_uh_foerste_registrert_dato": "uh_foerste_registrert_dato",
    "aar_uh_bachelor_foerste_registrert_dato": "uh_bachelor_foerste_registrert_dato",
    "aar_uh_master_foerste_registrert_dato": "uh_master_foerste_registrert_dato",
}

_REQUIRED_DATE_COLUMNS: list[str] = sorted(set(_YEAR_COLUMN_MAP.values()))


def _generate_microdata_registrert_foerste_view(
    alias: str,
    connection: db.DuckDBPyConnection,
) -> None:
    """Generate the `reg_foerste` Microdata dataset view.

    Only exposes the `aar_forste_reg_*`-year variables.
    """
    from nudb_use.datasets import NudbData
    from nudb_use.datasets.nudb_database import STRING_DTYPE
    from nudb_use.variables.derive.registrert_foerste import gr_foerste_registrert_dato
    from nudb_use.variables.derive.registrert_foerste import (
        uh_bachelor_foerste_registrert_dato,
    )
    from nudb_use.variables.derive.registrert_foerste import uh_foerste_registrert_dato
    from nudb_use.variables.derive.registrert_foerste import (
        uh_master_foerste_registrert_dato,
    )
    from nudb_use.variables.derive.registrert_foerste import vg_foerste_registrert_dato
    from nudb_use.variables.derive.registrert_foerste import (
        vg_foerste_registrert_erutdprogram_dato,
    )

    logger.info("Generating `_microdata_reg_foerste` dataset view.")

    # Henter ut ny utd_person for hver derive-funkjon
    cohort = NudbData("utd_person")
    base = cohort.select("snr").df()
    base["snr"] = base["snr"].astype(STRING_DTYPE)

    df = base.copy()

    # Hver derive-funksjon bruker en *fresh* snr-katalog kopi, slik at ikke
    # de forrige brukte utd_aktivitet_start/_slutt blir gjenbrukt.
    # Forhindrer at koden forventer at en person fyller hele hierarkiet med utdanninger,
    # og tillater at en person kan ha gjort noen aktiviteter, men ikke alle.
    func_to_col: dict[object, str] = {
        gr_foerste_registrert_dato: "gr_foerste_registrert_dato",
        vg_foerste_registrert_dato: "vg_foerste_registrert_dato",
        vg_foerste_registrert_erutdprogram_dato: "vg_foerste_registrert_erutdprogram_dato",
        uh_foerste_registrert_dato: "uh_foerste_registrert_dato",
        uh_bachelor_foerste_registrert_dato: "uh_bachelor_foerste_registrert_dato",
        uh_master_foerste_registrert_dato: "uh_master_foerste_registrert_dato",
    }

    for func, col_name in func_to_col.items():
        result = func(base.copy())
        if col_name in result.columns:
            df = df.merge(result[["snr", col_name]], on="snr", how="left")

    for date_col in _REQUIRED_DATE_COLUMNS:
        if date_col not in df.columns:
            df[date_col] = pd.Series([pd.NaT] * len(df), dtype="datetime64[s]")

    for year_col, source_col in _YEAR_COLUMN_MAP.items():
        df[year_col] = df[source_col].dt.year.astype("Int64")

    df = df[["snr", *_YEAR_COLUMN_MAP.keys()]]

    connection.register("_temp_registrert_foerste_df", df)
    connection.execute(
        f"CREATE OR REPLACE VIEW {alias} AS SELECT * FROM _temp_registrert_foerste_df"
    )
