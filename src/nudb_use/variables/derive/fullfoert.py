import datetime

import pandas as pd

from nudb_use.metadata.nudb_config.map_get_dtypes import BOOL_DTYPE_NAME
from nudb_use.metadata.nudb_config.map_get_dtypes import DTYPE_MAPPINGS

from .derive_decorator import wrap_derive
from .registrert import PRG_RANGES
from .registrert import raise_vg_utdprogram_outside_ranges

BOOL_DTYPE = DTYPE_MAPPINGS["pandas"][BOOL_DTYPE_NAME]
FULLFOERTKODE = "8"


__all__ = [
    "gr_ergrunnskole_fullfoert",
    "uh_erbachelor_fullfoert",
    "uh_erdoktorgrad_fullfoert",
    "uh_erhoeyskolekandidat_fullfoert",
    "uh_ermaster_fullfoert",
    "vg_erstudiespess_fullfoert",
    "vg_ervgo_fullfoert",
    "vg_eryrkesfag_fullfoert",
]


@wrap_derive
def gr_ergrunnskole_fullfoert(  # noqa: DOC101,DOC103,DOC201,DOC203
    df: pd.DataFrame,
) -> pd.Series:
    """Derive gr_ergrunnskole_fullfoert from nus2000, utd_fullfoertkode, utd_erutland."""
    return (
        (df["nus2000"].str[0] == "2")
        & (~df["utd_erutland"])
        & (df["utd_fullfoertkode"] == FULLFOERTKODE)
    ).astype(BOOL_DTYPE)


@wrap_derive
def vg_ervgo_fullfoert(  # noqa: DOC101,DOC103,DOC201,DOC203
    df: pd.DataFrame,
) -> pd.Series:
    """Derive vg_ervgo_fullfoert from nus2000, utd_fullfoertkode, vg_kompetanse_nus and utd_aktivitet_start."""
    return (
        (df["nus2000"].str[0].isin(["4", "5"]))
        & (df["utd_fullfoertkode"] == FULLFOERTKODE)
        & (
            (df["vg_kompetanse_nus"].isin(["1", "2", "3", "5"]))
            | (
                df["utd_aktivitet_start"]
                < datetime.datetime.strptime("2000-08-01", r"%Y-%m-%d")
            )  # Komp ikke utylt før 2000?
        )
    ).astype(BOOL_DTYPE)


@wrap_derive
def vg_erstudiespess_fullfoert(  # noqa: DOC101,DOC103,DOC201,DOC203
    df: pd.DataFrame,
) -> pd.Series:
    """Derive vg_erstudiespess_fullfoert from nus2000, vg_utdprogram, utd_fullfoertkode, vg_kompetanse_nus and utd_aktivitet_start."""
    raise_vg_utdprogram_outside_ranges(df["vg_utdprogram"])
    return (
        vg_ervgo_fullfoert(df)["vg_ervgo_fullfoert"]
        & (df["vg_utdprogram"].isin(PRG_RANGES["studiespess"]))
    ).astype(BOOL_DTYPE)


@wrap_derive
def vg_eryrkesfag_fullfoert(  # noqa: DOC101,DOC103,DOC201,DOC203
    df: pd.DataFrame,
) -> pd.Series:
    """Derive vg_eryrkesfag_fullfoert from nus2000, vg_utdprogram, utd_fullfoertkode, vg_kompetanse_nus and utd_aktivitet_start."""
    raise_vg_utdprogram_outside_ranges(df["vg_utdprogram"])
    return (
        vg_ervgo_fullfoert(df)["vg_ervgo_fullfoert"]
        & (df["vg_utdprogram"].isin(PRG_RANGES["yrkesfag"]))
    ).astype(BOOL_DTYPE)


@wrap_derive
def uh_erhoeyskolekandidat_fullfoert(  # noqa: DOC101,DOC103,DOC201,DOC203
    df: pd.DataFrame,
) -> pd.Series:
    """Derive vg_eryrkesfag_fullfoert from nus2000, vg_utdprogram, utd_fullfoertkode, vg_kompetanse_nus and utd_aktivitet_start."""
    return (
        (df["nus2000"].str[0] == "6")
        & (df["utd_fullfoertkode"] == FULLFOERTKODE)
        & (df["utd_klassetrinn"].astype("Int64").isin([15, 16]))
        & (
            ~df["uh_gruppering_nus"].isin(["01", "02"])
        )  # Ikke "Forberedende prøver", eller "Lavere nivås utdanning"
    ).astype(BOOL_DTYPE)


@wrap_derive
def uh_erbachelor_fullfoert(  # noqa: DOC101,DOC103,DOC201,DOC203
    df: pd.DataFrame,
) -> pd.Series:
    """Derive uh_erbachelor_fullfoert from uh_gradmerke_nus."""
    from nudb_use.nudb_logger import logger

    logger.critical(df)
    return (
        (df["uh_gradmerke_nus"] == "B") & (df["utd_fullfoertkode"] == FULLFOERTKODE)
    ).astype(BOOL_DTYPE)


@wrap_derive
def uh_ermaster_fullfoert(  # noqa: DOC101,DOC103,DOC201,DOC203
    df: pd.DataFrame,
) -> pd.Series:
    """Derive uh_ermaster_fullfoert from nus2000, utd_fullfoertkode."""
    return (
        (df["nus2000"].str[0] == "7") & (df["utd_fullfoertkode"] == FULLFOERTKODE)
    ).astype(BOOL_DTYPE)


@wrap_derive
def uh_erdoktorgrad_fullfoert(  # noqa: DOC101,DOC103,DOC201,DOC203
    df: pd.DataFrame,
) -> pd.Series:
    """Derive uh_erdoktorgrad_fullfoert from nus2000, utd_fullfoertkode."""
    return (
        (df["nus2000"].str[0] == "8") & (df["utd_fullfoertkode"] == FULLFOERTKODE)
    ).astype(BOOL_DTYPE)
