import pandas as pd

from nudb_use.nudb_logger import logger

@wrap_derive_keyed
def gr_foerste_fullfoert_dato(df: pd.DataFrame) -> pd.DataFrame:
    """Derive gr_foerste_fullfoert_dato from the avslutta data.
    
    Returns:
        pd.DataFrame: A dataframe with a join key, and the generated data
    """
    logger.warning("When deriving this variable, it is important that you use all of the data available, not a subset of avslutta for example.")
    df_to_join: pd.DataFrame = (
        df[df["gr_ergrunnskole_fullfort"]]
        .sort_values("utd_aktivitet_slutt")
        .groupby("snr")
        .first()
        .rename({"utd_aktivitet_slutt": "gr_foerste_fullfoert_dato"})
        .astype({"gr_foerste_fullfoert_dato": "datetime64[s]"})
        [["snr", "gr_foerste_fullfoert_dato"]]
        )
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