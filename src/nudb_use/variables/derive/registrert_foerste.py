from __future__ import annotations

import pandas as pd

from nudb_use.nudb_logger import logger

from .all_data_helpers import enforce_datetime_s
from .derive_decorator import wrap_derive_join_all_data

__all__ = [
    "gr_foerste_registrert_dato",
    "uh_bachelor_foerste_registrert_dato",
    "uh_foerste_nus2000",
    "uh_foerste_registrert_dato",
    "uh_master_foerste_registrert_dato",
    "vg_foerste_registrert_dato",
]


@wrap_derive_join_all_data
def uh_foerste_nus2000(df: pd.DataFrame) -> pd.DataFrame:
    """Derive the first nus2000 a person has on UH-level.

    Args:
        df: Source dataset containing at least snr, nus2000, utd_aktivitet_start.

    Returns:
        pd.DataFrame: A column suitable for adding as a new column to the df.
    """
    variable_name = "uh_foerste_nus2000"

    # we prioritize true utd_aktivitet_start values first, but if unavailable
    # we use the other columns. For now we just keep the dates as is, but
    # maybe we should impute? E.g., if its utd_aktivitet_slutt in the spring
    # semester, it might make sense to make the utd_aktivitet_start value be
    # in the fall semester?
    df["utd_aktivitet_start"] = (
        df["utd_aktivitet_start"]
        .fillna(df["utd_aktivitet_slutt"])
        .fillna(df["uh_eksamen_dato"])
    )

    df_agg = (
        df[df["nus2000"].str[0].isin(["6", "7", "8"])]
        .sort_values(["utd_aktivitet_start"])
        .groupby("snr", as_index=False)
        .first()
        .rename(columns={"nus2000": variable_name})
    )

    return df_agg


def first_registered_date_per_snr(
    df: pd.DataFrame, variable_name: str, filter_var: str
) -> pd.DataFrame:
    """Create first registered date for each person through snr-column, by aggregating, then merging for original order.

    Args:
        df: The dataset containing the necessary filter_var, snr and utd_aktivitet_start.
        variable_name: The variable we will be constructing.
        filter_var: A boolean column that will be used to filter down to the valid rows.

    Returns:
        pd.DataFrame: The produced date column for the first date a person has done something within the boolean filter.
    """
    mask = df[filter_var].fillna(False).astype(bool)

    logger.debug("right (pre fillna)\n")
    logger.debug(df)

    if "utd_aktivitet_slutt" in df.columns:
        df["utd_aktivitet_start"] = df["utd_aktivitet_start"].fillna(
            df["utd_aktivitet_slutt"]
        )

    if "uh_eksamen_dato" in df.columns:
        df["utd_aktivitet_start"] = df["utd_aktivitet_start"].fillna(
            df["uh_eksamen_dato"]
        )

    logger.debug("right (pre aggregation)\n")
    logger.debug(df)

    df_agg = (
        df.loc[mask, ["snr", "utd_aktivitet_start"]]
        .sort_values(["snr", "utd_aktivitet_start"])
        .groupby("snr", as_index=False)
        .first()
        .rename(columns={"utd_aktivitet_start": variable_name})
    )

    logger.debug("right (after aggregation)\n")
    logger.debug(df_agg)

    df_agg[variable_name] = enforce_datetime_s(df_agg[variable_name])
    return df_agg


@wrap_derive_join_all_data
def gr_foerste_registrert_dato(df: pd.DataFrame) -> pd.DataFrame:
    """Derive gr_foerste_registrert_dato from avslutta.

    Args:
        df: Source dataset containing at least snr, gr_ergrunnskole_registrering, utd_aktivitet_start.

    Returns:
        pd.DataFrame: A column suitable for adding as a new column to the df.
    """
    variable_name = "gr_foerste_registrert_dato"
    filter_var = "gr_ergrunnskole_registrering"
    return first_registered_date_per_snr(
        df, variable_name=variable_name, filter_var=filter_var
    )


@wrap_derive_join_all_data
def vg_foerste_registrert_dato(df: pd.DataFrame) -> pd.DataFrame:
    """Derive vg_foerste_registrert_dato from avslutta.

    Args:
        df: Source dataset containing at least snr, vg_ervgo_registrering, utd_aktivitet_start.

    Returns:
        pd.DataFrame: A column suitable for adding as a new column to the df.
    """
    variable_name = "vg_foerste_registrert_dato"
    filter_var = "vg_ervgo_registrering"
    return first_registered_date_per_snr(
        df, variable_name=variable_name, filter_var=filter_var
    )


@wrap_derive_join_all_data
def uh_foerste_registrert_dato(df: pd.DataFrame) -> pd.DataFrame:
    """Derive uh_foerste_registrert_dato from avslutta.

    Args:
        df: Source dataset containing at least snr, uh_erhoeyereutd_registrering, utd_aktivitet_start.

    Returns:
        pd.DataFrame: A column suitable for adding as a new column to the df.
    """
    variable_name = "uh_foerste_registrert_dato"
    filter_var = "uh_erhoeyereutd_registrering"
    return first_registered_date_per_snr(
        df, variable_name=variable_name, filter_var=filter_var
    )


@wrap_derive_join_all_data
def uh_bachelor_foerste_registrert_dato(df: pd.DataFrame) -> pd.DataFrame:
    """Derive uh_bachelor_foerste_registrert_dato from avslutta.

    Args:
        df: Source dataset containing at least snr, uh_erbachelor_registrering, utd_aktivitet_start.

    Returns:
        pd.DataFrame: A column suitable for adding as a new column to the df.
    """
    variable_name = "uh_bachelor_foerste_registrert_dato"
    filter_var = "uh_erbachelor_registrering"
    return first_registered_date_per_snr(
        df, variable_name=variable_name, filter_var=filter_var
    )


@wrap_derive_join_all_data
def uh_master_foerste_registrert_dato(df: pd.DataFrame) -> pd.DataFrame:
    """Derive uh_master_foerste_registrert_dato from avslutta.

    Args:
        df: Source dataset containing at least snr, uh_ermaster_registrering, utd_aktivitet_start.

    Returns:
        pd.DataFrame: A column suitable for adding as a new column to the df.
    """
    variable_name = "uh_master_foerste_registrert_dato"
    filter_var = "uh_ermaster_registrering"
    return first_registered_date_per_snr(
        df, variable_name=variable_name, filter_var=filter_var
    )
