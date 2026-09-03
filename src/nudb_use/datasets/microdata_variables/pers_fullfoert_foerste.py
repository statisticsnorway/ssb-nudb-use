import duckdb as db
import pandas as pd

from nudb_use.nudb_logger import logger
# Mapper fullføringsår til datokolonne. 
_YEAR_COLUMN_MAP: dict[str, str] = {
    "aar_forste_fullf_gs": "gr_foerste_fullfoert_dato",
    "aar_forste_fullf_vs": "vg_foerste_fullfoert_dato",
    "aar_forste_fullf_vs_lov": "vg_foerste_fullfoert_dato",
    "aar_forste_fullf_vsa": "vg_studiespess_foerste_fullfoert_dato",
    "aar_forste_fullf_vsa_lov": "vg_studiespess_foerste_fullfoert_dato",
    "aar_forste_fullf_vsy": "vg_yrkesfag_foerste_fullfoert_dato",
    "aar_forste_fullf_vsy_lov": "vg_yrkesfag_foerste_fullfoert_dato",
    "aar_forste_fullf_hoy": "uh_hoeyskolekandidat_foerste_fullfoert_dato",
    "aar_forste_fullf_bach": "uh_bachelor_foerste_fullfoert_dato",
    "aar_forste_fullf_hov": "uh_master_foerste_fullfoert_dato",
    "aar_forste_fullf_cmg": "uh_master_foerste_fullfoert_dato",
    "aar_forste_fullf_dok": "uh_doktorgrad_foerste_fullfoert_dato",
}

_REQUIRED_DATE_COLUMNS: list[str] = sorted(set(_YEAR_COLUMN_MAP.values()))


def _generate_microdata_fullfoert_foerste_view(
    alias: str,
    connection: db.DuckDBPyConnection,
) -> None:
    """Generate the `fullfoert_foerste` Microdata dataset view.

    Only exposes the `aar_forste_fullf_*` completion-year variables.
    """
    from nudb_use.datasets import NudbData
    from nudb_use.datasets.nudb_database import STRING_DTYPE
    from nudb_use.variables.derive.fullfoert_foerste import (
        gr_foerste_fullfoert_dato,
        uh_bachelor_foerste_fullfoert_dato,
        uh_doktorgrad_foerste_fullfoert_dato,
        uh_hoeyskolekandidat_foerste_fullfoert_dato,
        uh_master_foerste_fullfoert_dato,
        vg_foerste_fullfoert_dato,
        vg_studiespess_foerste_fullfoert_dato,
        vg_yrkesfag_foerste_fullfoert_dato,
    )

    logger.info("Generating `_microdata_fullfoert_foerste` dataset view.")

    cohort = NudbData("utd_person")
    base = cohort.select("snr").df()
    base["snr"] = base["snr"].astype(STRING_DTYPE)

    df = base.copy()
    
    # Hver derive-funksjon bruker en *fresh* snr-katalog kopi, slik at ikke
    # de forrige brukte utd_aktivitet_start/_slutt blir gjenbrukt. 
    # Forhindrer at koden forventer en "full-hierarchical-structure", og er
    # åpen for en "sparse-hierarchical-structure. "
    func_to_col: dict[object, str] = {
        gr_foerste_fullfoert_dato: "gr_foerste_fullfoert_dato",
        vg_foerste_fullfoert_dato: "vg_foerste_fullfoert_dato",
        vg_studiespess_foerste_fullfoert_dato: "vg_studiespess_foerste_fullfoert_dato",
        vg_yrkesfag_foerste_fullfoert_dato: "vg_yrkesfag_foerste_fullfoert_dato",
        uh_hoeyskolekandidat_foerste_fullfoert_dato: (
            "uh_hoeyskolekandidat_foerste_fullfoert_dato"
        ),
        uh_bachelor_foerste_fullfoert_dato: "uh_bachelor_foerste_fullfoert_dato",
        uh_master_foerste_fullfoert_dato: "uh_master_foerste_fullfoert_dato",
        uh_doktorgrad_foerste_fullfoert_dato: "uh_doktorgrad_foerste_fullfoert_dato",
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

    connection.register("_temp_fullfoert_foerste_df", df)
    connection.execute(
        f"CREATE OR REPLACE VIEW {alias} AS SELECT * FROM _temp_fullfoert_foerste_df"
    )