import pandas as pd
from collections.abc import Callable

from nudb_use.nudb_logger import logger

from __future__ import annotations

from pathlib import Path
from typing import Callable, Sequence

from .all_data_helpers import get_source_data, enforce_datetime_s, join_variable_data


def gr_foerste_fullfoert_dato(data_to_merge: pd.DataFrame | None = None) -> pd.DataFrame:
    """Derive gr_foerste_fullfoert_dato from avslutta.

    Args:
        avslutta: Source dataset containing at least snr, gr_ergrunnskole_fullfort, utd_aktivitet_slutt.

    Returns:
        Dataframe with columns ["snr", "gr_foerste_fullfoert_dato"] suitable for joining.
    """
    variable_name = "gr_foerste_fullfoert_dato"
    source_data = get_source_data(variable_name, data_to_merge, filter_vars=["gr_ergrunnskole_fullfort"])

    mask = source_data["gr_ergrunnskole_fullfort"].fillna(False).astype(bool)

    df_to_join = (
        source_data.loc[mask, ["snr", "utd_aktivitet_slutt"]]
        .sort_values(["snr", "utd_aktivitet_slutt"])
        .groupby("snr", as_index=False)
        .first()
        .rename(columns={"utd_aktivitet_slutt": variable_name})
    )

    # Enforce dtype (DuckDB might already give datetime64[ns], but safe to normalize)
    df_to_join[variable_name] = enforce_datetime_s(df_to_join[variable_name])

    if data_to_merge is not None:
        return join_variable_data(variable_name, df_to_join, data_to_merge)
    return df_to_join

#@wrap_derive
#def vg_foerste_fullfoert_dato(df: pd.DataFrame) -> pd.Series:
#    date_col: pd.Series = .astype("datetime64[s]")
#    return date_col
#
#@wrap_derive
#def vg_studiespess_foerste_fullfoert_dato(df: pd.DataFrame) -> pd.Series:
#    date_col: pd.Series = .astype("datetime64[s]")
#    return date_col
#
#@wrap_derive
#def vg_yrkesfag_foerste_fullfoert_dato(df: pd.DataFrame) -> pd.Series:
#    date_col: pd.Series = .astype("datetime64[s]")
#    return date_col
#
#@wrap_derive
#def uh_hoyskolekandidat_foerste_fullfoert_dato(df: pd.DataFrame) -> pd.Series:
#    date_col: pd.Series = .astype("datetime64[s]")
#    return date_col
#
#@wrap_derive
#def uh_bachelor_foerste_fullfoert_dato(df: pd.DataFrame) -> pd.Series:
#    date_col: pd.Series = .astype("datetime64[s]")
#    return date_col
#
#@wrap_derive
#def uh_master_foerste_registrert_dato(df: pd.DataFrame) -> pd.Series:
#    date_col: pd.Series = .astype("datetime64[s]")
#    return date_col
#
#@wrap_derive
#def uh_master_foerste_fullfoert_dato(df: pd.DataFrame) -> pd.Series:
#    date_col: pd.Series = .astype("datetime64[s]")
#    return date_col
#
#@wrap_derive
#def uh_doktorgrad_foerste_fullfoert_dato(df: pd.DataFrame) -> pd.Series:
#    date_col: pd.Series = .astype("datetime64[s]")
#    return date_col