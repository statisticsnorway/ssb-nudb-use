from __future__ import annotations

import pandas as pd

from .all_data_helpers import enforce_datetime_s
from .derive_decorator import wrap_derive_join_all_data

__all__ = [
    "gr_foerste_fullfoert_dato",
    "uh_bachelor_foerste_fullfoert_dato",
    "uh_doktorgrad_foerste_fullfoert_dato",
    "uh_hoeyskolekandidat_foerste_fullfoert_dato",
    "uh_master_foerste_fullfoert_dato",
    "vg_foerste_fullfoert_dato",
    "vg_studiespess_foerste_fullfoert_dato",
    "vg_yrkesfag_foerste_fullfoert_dato",
]


def first_end_date_per_snr(
    df: pd.DataFrame, variable_name: str, filter_var: str
) -> pd.DataFrame:
    """Create first ended date for each person through snr-column, by aggregating, then merging for original order.

    Args:
        df: The dataset containing the necessary filter_var, snr and utd_aktivitet_slutt.
        variable_name: The variable we will be constructing.
        filter_var: A boolean column that will be used to filter down to the valid rows.

    Returns:
        pd.DataFrame: The produced date column for the first date a person has done something within the boolean filter.
    """
    mask = df[filter_var].fillna(False).astype(bool)
    df_agg = (
        df.loc[mask, ["snr", "utd_aktivitet_slutt"]]
        .sort_values(["snr", "utd_aktivitet_slutt"])
        .groupby("snr", as_index=False)
        .first()
        .rename(columns={"utd_aktivitet_slutt": variable_name})
    )
    df_agg[variable_name] = enforce_datetime_s(df_agg[variable_name])

    return df_agg


@wrap_derive_join_all_data
def gr_foerste_fullfoert_dato(df: pd.DataFrame) -> pd.DataFrame:
    """Derive gr_foerste_fullfoert_dato from avslutta.

    Args:
        df: Source dataset containing at least snr, gr_ergrunnskole_fullfoert, utd_aktivitet_slutt.

    Returns:
        pd.DataFrame: A column suitable for adding as a new column to the df.
    """
    variable_name = "gr_foerste_fullfoert_dato"
    filter_var = "gr_ergrunnskole_fullfoert"
    return first_end_date_per_snr(
        df, variable_name=variable_name, filter_var=filter_var
    )


@wrap_derive_join_all_data
def vg_foerste_fullfoert_dato(df: pd.DataFrame) -> pd.DataFrame:
    """Derive vg_foerste_fullfoert_dato from avslutta.

    Args:
        df: Source dataset containing at least snr, vg_ervgo_fullfoert, utd_aktivitet_slutt.

    Returns:
        pd.DataFrame: A column suitable for adding as a new column to the df.
    """
    variable_name = "vg_foerste_fullfoert_dato"
    filter_var = "vg_ervgo_fullfoert"
    return first_end_date_per_snr(
        df, variable_name=variable_name, filter_var=filter_var
    )


@wrap_derive_join_all_data
def vg_studiespess_foerste_fullfoert_dato(df: pd.DataFrame) -> pd.DataFrame:
    """Derive vg_studiespess_foerste_fullfoert_dato from avslutta.

    Args:
        df: Source dataset containing at least snr, vg_erstudiespess_fullfoert, utd_aktivitet_slutt.

    Returns:
        pd.DataFrame: A column suitable for adding as a new column to the df.
    """
    variable_name = "vg_studiespess_foerste_fullfoert_dato"
    filter_var = "vg_erstudiespess_fullfoert"
    return first_end_date_per_snr(
        df, variable_name=variable_name, filter_var=filter_var
    )


@wrap_derive_join_all_data
def vg_yrkesfag_foerste_fullfoert_dato(df: pd.DataFrame) -> pd.DataFrame:
    """Derive vg_yrkesfag_foerste_fullfoert_dato from avslutta.

    Args:
        df: Source dataset containing at least snr, vg_eryrkesfag_fullfoert, utd_aktivitet_slutt.

    Returns:
        pd.DataFrame: A column suitable for adding as a new column to the df.
    """
    variable_name = "vg_yrkesfag_foerste_fullfoert_dato"
    filter_var = "vg_eryrkesfag_fullfoert"
    return first_end_date_per_snr(
        df, variable_name=variable_name, filter_var=filter_var
    )


@wrap_derive_join_all_data
def uh_hoeyskolekandidat_foerste_fullfoert_dato(df: pd.DataFrame) -> pd.DataFrame:
    """Derive uh_hoeyskolekandidat_foerste_fullfoert_dato from avslutta.

    Args:
        df: Source dataset containing at least snr, uh_erhoeyskolekandidat_fullfoert, utd_aktivitet_slutt.

    Returns:
        pd.DataFrame: A column suitable for adding as a new column to the df.
    """
    variable_name = "uh_hoeyskolekandidat_foerste_fullfoert_dato"
    filter_var = "uh_erhoeyskolekandidat_fullfoert"
    return first_end_date_per_snr(
        df, variable_name=variable_name, filter_var=filter_var
    )


@wrap_derive_join_all_data
def uh_bachelor_foerste_fullfoert_dato(df: pd.DataFrame) -> pd.DataFrame:
    """Derive uh_bachelor_foerste_fullfoert_dato from avslutta.

    Args:
        df: Source dataset containing at least snr, uh_erbachelor_fullfoert, utd_aktivitet_slutt.

    Returns:
        pd.DataFrame: A column suitable for adding as a new column to the df.
    """
    variable_name = "uh_bachelor_foerste_fullfoert_dato"
    filter_var = "uh_erbachelor_fullfoert"
    return first_end_date_per_snr(
        df, variable_name=variable_name, filter_var=filter_var
    )


@wrap_derive_join_all_data
def uh_master_foerste_fullfoert_dato(df: pd.DataFrame) -> pd.DataFrame:
    """Derive uh_master_foerste_fullfoert_dato from avslutta.

    Args:
        df: Source dataset containing at least snr, uh_ermaster_fullfoert, utd_aktivitet_slutt.

    Returns:
        pd.DataFrame: A column suitable for adding as a new column to the df.
    """
    variable_name = "uh_master_foerste_fullfoert_dato"
    filter_var = "uh_ermaster_fullfoert"
    return first_end_date_per_snr(
        df, variable_name=variable_name, filter_var=filter_var
    )


@wrap_derive_join_all_data
def uh_doktorgrad_foerste_fullfoert_dato(df: pd.DataFrame) -> pd.DataFrame:
    """Derive uh_doktorgrad_foerste_fullfoert_dato from avslutta.

    Args:
        df: Source dataset containing at least snr, uh_erdoktorgrad_fullfoert, utd_aktivitet_slutt.

    Returns:
        pd.DataFrame: A column suitable for adding as a new column to the df.
    """
    variable_name = "uh_doktorgrad_foerste_fullfoert_dato"
    filter_var = "uh_erdoktorgrad_fullfoert"
    return first_end_date_per_snr(
        df, variable_name=variable_name, filter_var=filter_var
    )
