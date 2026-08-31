"""Module with NudbData generators for Microdata person-level variables."""

from collections import defaultdict
from collections.abc import Callable

import duckdb as db
import pandas as pd

from nudb_use.nudb_logger import logger

_igang_cache: pd.DataFrame | None = None


def get_igang_df() -> pd.DataFrame:
    """Get igang DataFrame with caching."""
    global _igang_cache
    if _igang_cache is None:
        from nudb_use.datasets import NudbData

        igang = NudbData("igang")
        _igang_cache = igang.select(
            "snr, utd_aktivitet_start, nus2000, vg_utdprogram"
        ).df()
    return _igang_cache


def compute_semesters(start_col: str, end_col: str, df: pd.DataFrame) -> pd.Series:
    """Calculate the calendar semesters spent between start_col and end_col."""
    start_dates = pd.to_datetime(df[start_col], errors="coerce")
    end_dates = pd.to_datetime(df[end_col], errors="coerce")

    start_idx = start_dates.dt.year * 2 + (start_dates.dt.month >= 8).astype("Int64")
    end_idx = end_dates.dt.year * 2 + (end_dates.dt.month >= 8).astype("Int64")

    return (end_idx - start_idx).astype("Int64")


def compute_active_semesters(
    start_col: str,
    end_col: str,
    level_mask_func: Callable[[pd.DataFrame], pd.Series],
    df: pd.DataFrame,
) -> pd.Series:
    """Count unique active semesters in `igang` between start_col and end_col."""
    igang_df = get_igang_df()

    # Filter igang_df to requested level
    igang_filtered = igang_df[level_mask_func(igang_df)].copy()

    # Get semester index for each igang row
    igang_filtered["semester_idx"] = igang_filtered[
        "utd_aktivitet_start"
    ].dt.year * 2 + (igang_filtered["utd_aktivitet_start"].dt.month >= 8).astype(
        "Int64"
    )

    # Keep only unique snr + semester_idx to avoid counting multiple courses in same semester
    igang_filtered = igang_filtered[["snr", "semester_idx"]].drop_duplicates()

    # Map snr to list of registered semester_indices
    snr_to_semesters = defaultdict(list)
    for snr, s_idx in zip(
        igang_filtered["snr"], igang_filtered["semester_idx"], strict=False
    ):
        if pd.notna(s_idx):
            snr_to_semesters[snr].append(s_idx)

    # Compute start/end indices for the cohorts
    start_idx = df[start_col].dt.year * 2 + (df[start_col].dt.month >= 8).astype(
        "Int64"
    )
    end_idx = df[end_col].dt.year * 2 + (df[end_col].dt.month >= 8).astype("Int64")

    # Count semesters
    counts = []
    for snr, s_val, e_val in zip(df["snr"], start_idx, end_idx, strict=False):
        if pd.isna(s_val) or pd.isna(e_val):
            counts.append(pd.NA)
            continue
        sems = snr_to_semesters.get(snr, [])
        valid_sems = [sem for sem in sems if s_val <= sem <= e_val]
        counts.append(len(valid_sems))

    return pd.Series(counts, index=df.index).astype("Int64")


def _generate_microdata_personvariabler_view(
    alias: str,
    connection: db.DuckDBPyConnection,
) -> None:
    """Generate the unified personvariabler dataset for MicroData."""
    from nudb_use.datasets import NudbData
    from nudb_use.variables import derive
    from nudb_use.variables.derive.registrert import PRG_RANGES

    logger.info("Generating '_microdata_personvariabler' dataset view.")

    # 1. Start with the core cohort of persons (from utd_person)
    cohort = NudbData("utd_person")
    df = cohort.df()

    # 2. Derive Background & Family Level Variables
    df = derive.utd_foreldres_utdnivaa_16aar(df)
    df["sosbak"] = df.get(
        "utd_foreldres_utdnivaa_16aar", pd.Series([pd.NA] * len(df), dtype="string")
    )
    df = derive.utd_hoeyeste_far_nus2000(df)
    df["nus2000_far_16"] = df.get(
        "utd_hoeyeste_far_nus2000", pd.Series([pd.NA] * len(df), dtype="string")
    )
    df = derive.utd_hoeyeste_mor_nus2000(df)
    df["nus2000_mor_16"] = df.get(
        "utd_hoeyeste_mor_nus2000", pd.Series([pd.NA] * len(df), dtype="string")
    )
    df = derive.pers_bokommune_16aar(df)
    df["komm_16"] = df.get(
        "pers_bokommune_16aar", pd.Series([pd.NA] * len(df), dtype="string")
    )

    # 3. Derive Date Fields via Core Derivations (which fetch and join avslutta/igang data)
    from nudb_use.variables.derive.fullfoert_foerste import gr_foerste_fullfoert_dato
    from nudb_use.variables.derive.fullfoert_foerste import (
        uh_bachelor_foerste_fullfoert_dato,
    )
    from nudb_use.variables.derive.fullfoert_foerste import (
        uh_doktorgrad_foerste_fullfoert_dato,
    )
    from nudb_use.variables.derive.fullfoert_foerste import (
        uh_hoeyskolekandidat_foerste_fullfoert_dato,
    )
    from nudb_use.variables.derive.fullfoert_foerste import (
        uh_master_foerste_fullfoert_dato,
    )
    from nudb_use.variables.derive.fullfoert_foerste import vg_foerste_fullfoert_dato
    from nudb_use.variables.derive.fullfoert_foerste import (
        vg_studiespess_foerste_fullfoert_dato,
    )
    from nudb_use.variables.derive.fullfoert_foerste import (
        vg_yrkesfag_foerste_fullfoert_dato,
    )
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

    df = df.merge(gr_foerste_fullfoert_dato(df), on="snr", how="left")
    df = df.merge(vg_foerste_fullfoert_dato(df), on="snr", how="left")
    df = df.merge(vg_studiespess_foerste_fullfoert_dato(df), on="snr", how="left")
    df = df.merge(vg_yrkesfag_foerste_fullfoert_dato(df), on="snr", how="left")
    df = df.merge(uh_hoeyskolekandidat_foerste_fullfoert_dato(df), on="snr", how="left")
    df = df.merge(uh_bachelor_foerste_fullfoert_dato(df), on="snr", how="left")
    df = df.merge(uh_master_foerste_fullfoert_dato(df), on="snr", how="left")
    df = df.merge(uh_doktorgrad_foerste_fullfoert_dato(df), on="snr", how="left")

    df = df.merge(vg_foerste_registrert_dato(df), on="snr", how="left")
    df = df.merge(vg_foerste_registrert_erutdprogram_dato(df), on="snr", how="left")
    df = df.merge(uh_foerste_registrert_dato(df), on="snr", how="left")
    df = df.merge(uh_bachelor_foerste_registrert_dato(df), on="snr", how="left")
    df = df.merge(uh_master_foerste_registrert_dato(df), on="snr", how="left")

    # Ensure all required date columns are present in df (defensive check)
    for date_col in [
        "gr_foerste_fullfoert_dato",
        "vg_foerste_fullfoert_dato",
        "vg_studiespess_foerste_fullfoert_dato",
        "vg_yrkesfag_foerste_fullfoert_dato",
        "uh_hoeyskolekandidat_foerste_fullfoert_dato",
        "uh_bachelor_foerste_fullfoert_dato",
        "uh_master_foerste_fullfoert_dato",
        "uh_doktorgrad_foerste_fullfoert_dato",
        "vg_foerste_registrert_dato",
        "vg_foerste_registrert_erutdprogram_dato",
        "uh_foerste_registrert_dato",
        "uh_bachelor_foerste_registrert_dato",
        "uh_master_foerste_registrert_dato",
    ]:
        if date_col not in df.columns:
            df[date_col] = pd.Series([pd.NaT] * len(df), dtype="datetime64[s]")

    # 4. Extract Completion Years
    df["aar_forste_fullf_gs"] = df["gr_foerste_fullfoert_dato"].dt.year.astype("Int64")
    df["aar_forste_fullf_vs"] = df["vg_foerste_fullfoert_dato"].dt.year.astype("Int64")
    df["aar_forste_fullf_vs_lov"] = df["vg_foerste_fullfoert_dato"].dt.year.astype(
        "Int64"
    )
    df["aar_forste_fullf_vsa"] = df[
        "vg_studiespess_foerste_fullfoert_dato"
    ].dt.year.astype("Int64")
    df["aar_forste_fullf_vsa_lov"] = df[
        "vg_studiespess_foerste_fullfoert_dato"
    ].dt.year.astype("Int64")
    df["aar_forste_fullf_vsy"] = df[
        "vg_yrkesfag_foerste_fullfoert_dato"
    ].dt.year.astype("Int64")
    df["aar_forste_fullf_vsy_lov"] = df[
        "vg_yrkesfag_foerste_fullfoert_dato"
    ].dt.year.astype("Int64")
    df["aar_forste_fullf_hoy"] = df[
        "uh_hoeyskolekandidat_foerste_fullfoert_dato"
    ].dt.year.astype("Int64")
    df["aar_forste_fullf_bach"] = df[
        "uh_bachelor_foerste_fullfoert_dato"
    ].dt.year.astype("Int64")
    df["aar_forste_fullf_hov"] = df["uh_master_foerste_fullfoert_dato"].dt.year.astype(
        "Int64"
    )
    df["aar_forste_fullf_cmg"] = df["uh_master_foerste_fullfoert_dato"].dt.year.astype(
        "Int64"
    )
    df["aar_forste_fullf_dok"] = df[
        "uh_doktorgrad_foerste_fullfoert_dato"
    ].dt.year.astype("Int64")

    # 5. Extract Registration Years & Age
    df["aar_ny_i_vid_utd"] = df["vg_foerste_registrert_dato"].dt.year.astype("Int64")
    df["aar_ny_i_vid_utd_lov"] = df[
        "vg_foerste_registrert_erutdprogram_dato"
    ].dt.year.astype("Int64")
    df["aar_forste_reg_uh"] = df["uh_foerste_registrert_dato"].dt.year.astype("Int64")
    df["aar_forste_reg_hov"] = df["uh_master_foerste_registrert_dato"].dt.year.astype(
        "Int64"
    )

    foedselsdato_col = None
    for c in ["foedselsdato", "pers_foedselsdato"]:
        if c in df.columns:
            foedselsdato_col = c
            break

    if foedselsdato_col:
        df["alder_forste_reg_uh"] = (
            df["aar_forste_reg_uh"] - df[foedselsdato_col].dt.year
        ).astype("Int64")
    else:
        df["alder_forste_reg_uh"] = pd.Series([pd.NA] * len(df), dtype="Int64")

    # 6. Compute Semesters Spent (Calendar Semesters)
    df["semester_fff_vs"] = compute_semesters(
        "vg_foerste_registrert_dato", "vg_foerste_fullfoert_dato", df
    )
    df["semester_fff_vs_lov"] = compute_semesters(
        "vg_foerste_registrert_erutdprogram_dato",
        "vg_foerste_fullfoert_dato",
        df,
    )
    df["semester_fff_vsa"] = compute_semesters(
        "vg_foerste_registrert_dato", "vg_studiespess_foerste_fullfoert_dato", df
    )
    df["semester_fff_vsa_lov"] = compute_semesters(
        "vg_foerste_registrert_erutdprogram_dato",
        "vg_studiespess_foerste_fullfoert_dato",
        df,
    )
    df["semester_fff_vsy"] = compute_semesters(
        "vg_foerste_registrert_dato", "vg_yrkesfag_foerste_fullfoert_dato", df
    )
    df["semester_fff_vsy_lov"] = compute_semesters(
        "vg_foerste_registrert_erutdprogram_dato",
        "vg_yrkesfag_foerste_fullfoert_dato",
        df,
    )
    df["semester_fff_hoy"] = compute_semesters(
        "uh_foerste_registrert_dato",
        "uh_hoeyskolekandidat_foerste_fullfoert_dato",
        df,
    )
    df["semester_fff_bach"] = compute_semesters(
        "uh_bachelor_foerste_registrert_dato",
        "uh_bachelor_foerste_fullfoert_dato",
        df,
    )
    df["semester_fff_cmg"] = compute_semesters(
        "uh_foerste_registrert_dato", "uh_master_foerste_fullfoert_dato", df
    )
    df["semester_fff_hov"] = compute_semesters(
        "uh_master_foerste_registrert_dato",
        "uh_master_foerste_fullfoert_dato",
        df,
    )
    df["semester_fff_dok"] = compute_semesters(
        "uh_foerste_registrert_dato", "uh_doktorgrad_foerste_fullfoert_dato", df
    )

    # 7. Compute Semesters Total (Active Registered Semesters)
    df["semester_tot_fff_vs"] = compute_active_semesters(
        "vg_foerste_registrert_dato",
        "vg_foerste_fullfoert_dato",
        lambda ig_df: ig_df["nus2000"].str[0].isin(["3", "4"]),
        df,
    )
    df["semester_tot_fff_vs_lov"] = compute_active_semesters(
        "vg_foerste_registrert_erutdprogram_dato",
        "vg_foerste_fullfoert_dato",
        lambda ig_df: ig_df["nus2000"].str[0].isin(["3", "4"]),
        df,
    )
    df["semester_tot_fff_vsa"] = compute_active_semesters(
        "vg_foerste_registrert_dato",
        "vg_studiespess_foerste_fullfoert_dato",
        lambda ig_df: ig_df["nus2000"].str[0].isin(["3", "4"])
        & ig_df["vg_utdprogram"].isin(PRG_RANGES["studiespess"]),
        df,
    )
    df["semester_tot_fff_vsa_lov"] = compute_active_semesters(
        "vg_foerste_registrert_erutdprogram_dato",
        "vg_studiespess_foerste_fullfoert_dato",
        lambda ig_df: ig_df["nus2000"].str[0].isin(["3", "4"])
        & ig_df["vg_utdprogram"].isin(PRG_RANGES["studiespess"]),
        df,
    )
    df["semester_tot_fff_vsy"] = compute_active_semesters(
        "vg_foerste_registrert_dato",
        "vg_yrkesfag_foerste_fullfoert_dato",
        lambda ig_df: ig_df["nus2000"].str[0].isin(["3", "4"])
        & ig_df["vg_utdprogram"].isin(PRG_RANGES["yrkesfag"]),
        df,
    )
    df["semester_tot_fff_vsy_lov"] = compute_active_semesters(
        "vg_foerste_registrert_erutdprogram_dato",
        "vg_yrkesfag_foerste_fullfoert_dato",
        lambda ig_df: ig_df["nus2000"].str[0].isin(["3", "4"])
        & ig_df["vg_utdprogram"].isin(PRG_RANGES["yrkesfag"]),
        df,
    )
    df["semester_tot_fff_hoy"] = compute_active_semesters(
        "uh_foerste_registrert_dato",
        "uh_hoeyskolekandidat_foerste_fullfoert_dato",
        lambda ig_df: ig_df["nus2000"].str[0].isin(["6", "7", "8"]),
        df,
    )
    df["semester_tot_fff_bach"] = compute_active_semesters(
        "uh_bachelor_foerste_registrert_dato",
        "uh_bachelor_foerste_fullfoert_dato",
        lambda ig_df: ig_df["nus2000"].str[0].isin(["6", "7", "8"]),
        df,
    )
    df["semester_tot_fff_cmg"] = compute_active_semesters(
        "uh_foerste_registrert_dato",
        "uh_master_foerste_fullfoert_dato",
        lambda ig_df: ig_df["nus2000"].str[0].isin(["6", "7", "8"]),
        df,
    )
    df["semester_tot_fff_hov"] = compute_active_semesters(
        "uh_master_foerste_registrert_dato",
        "uh_master_foerste_fullfoert_dato",
        lambda ig_df: ig_df["nus2000"].str[0].isin(["6", "7", "8"]),
        df,
    )
    df["semester_tot_fff_dok"] = compute_active_semesters(
        "uh_foerste_registrert_dato",
        "uh_doktorgrad_foerste_fullfoert_dato",
        lambda ig_df: ig_df["nus2000"].str[0].isin(["6", "7", "8"]),
        df,
    )

    # 8. Register resulting DataFrame as a DuckDB view
    connection.register("_temp_person_df", df)
    connection.execute(
        f"CREATE OR REPLACE VIEW {alias} AS SELECT * FROM _temp_person_df"
    )
