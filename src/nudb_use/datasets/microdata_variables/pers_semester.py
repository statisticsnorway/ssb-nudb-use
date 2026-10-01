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


def _generate_microdata_pers_semester_view(
    alias: str,
    connection: db.DuckDBPyConnection,
) -> None:
    """Generate the `pers_semester` Microdata dataset view.

    Only exposes the `semester_fff_*` (elapsed calendar semesters) and
    `semester_tot_fff_*` (active registered semesters) variables.
    """
    from nudb_use.datasets import NudbData
    from nudb_use.datasets.nudb_database import STRING_DTYPE
    from nudb_use.variables.derive.registrert import PRG_RANGES
    from nudb_use.variables.derive.registrert_foerste import (
        uh_bachelor_foerste_registrert_dato,
        uh_foerste_registrert_dato,
        uh_master_foerste_registrert_dato,
        vg_foerste_registrert_dato,
        vg_foerste_registrert_erutdprogram_dato,
    )
    from nudb_use.variables.derive.fullfoert_foerste import (
        uh_bachelor_foerste_fullfoert_dato,
        uh_doktorgrad_foerste_fullfoert_dato,
        uh_hoeyskolekandidat_foerste_fullfoert_dato,
        uh_master_foerste_fullfoert_dato,
        vg_foerste_fullfoert_dato,
        vg_studiespess_foerste_fullfoert_dato,
        vg_yrkesfag_foerste_fullfoert_dato,
    )

    logger.info("Generating `_microdata_pers_semester` dataset view.")

    # Start with the core cohort of persons (from utd_person)
    cohort = NudbData("utd_person")
    base = cohort.select("snr").df()
    base["snr"] = base["snr"].astype(STRING_DTYPE)

    df = base.copy()

    # Define function to column mappings
    func_to_col = {
        vg_foerste_registrert_dato: "vg_foerste_registrert_dato",
        vg_foerste_registrert_erutdprogram_dato: "vg_foerste_registrert_erutdprogram_dato",
        vg_foerste_fullfoert_dato: "vg_foerste_fullfoert_dato",
        vg_studiespess_foerste_fullfoert_dato: "vg_studiespess_foerste_fullfoert_dato",
        vg_yrkesfag_foerste_fullfoert_dato: "vg_yrkesfag_foerste_fullfoert_dato",
        uh_foerste_registrert_dato: "uh_foerste_registrert_dato",
        uh_hoeyskolekandidat_foerste_fullfoert_dato: "uh_hoeyskolekandidat_foerste_fullfoert_dato",
        uh_bachelor_foerste_registrert_dato: "uh_bachelor_foerste_registrert_dato",
        uh_bachelor_foerste_fullfoert_dato: "uh_bachelor_foerste_fullfoert_dato",
        uh_master_foerste_registrert_dato: "uh_master_foerste_registrert_dato",
        uh_master_foerste_fullfoert_dato: "uh_master_foerste_fullfoert_dato",
        uh_doktorgrad_foerste_fullfoert_dato: "uh_doktorgrad_foerste_fullfoert_dato",
    }

    # Derive each date column using a fresh base dataframe copy
    for func, col_name in func_to_col.items():
        result = func(base.copy())
        if col_name in result.columns:
            df = df.merge(result[["snr", col_name]], on="snr", how="left")

    # Defensive check: ensure all required date columns are present
    required_date_cols = list(func_to_col.values())
    for date_col in required_date_cols:
        if date_col not in df.columns:
            df[date_col] = pd.Series([pd.NaT] * len(df), dtype="datetime64[s]")

    # Compute Semesters Spent (Calendar Semesters)
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

    # Compute Semesters Total (Active Registered Semesters)
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

    semester_cols = [
        "semester_fff_vs",
        "semester_fff_vs_lov",
        "semester_fff_vsa",
        "semester_fff_vsa_lov",
        "semester_fff_vsy",
        "semester_fff_vsy_lov",
        "semester_fff_hoy",
        "semester_fff_bach",
        "semester_fff_cmg",
        "semester_fff_hov",
        "semester_fff_dok",
        "semester_tot_fff_vs",
        "semester_tot_fff_vs_lov",
        "semester_tot_fff_vsa",
        "semester_tot_fff_vsa_lov",
        "semester_tot_fff_vsy",
        "semester_tot_fff_vsy_lov",
        "semester_tot_fff_hoy",
        "semester_tot_fff_bach",
        "semester_tot_fff_cmg",
        "semester_tot_fff_hov",
        "semester_tot_fff_dok",
    ]

    df = df[["snr", *semester_cols]]

    connection.register("_temp_pers_semester_df", df)
    connection.execute(
        f"CREATE OR REPLACE VIEW {alias} AS SELECT * FROM _temp_pers_semester_df"
    )
