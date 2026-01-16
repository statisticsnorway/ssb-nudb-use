import datetime

import pandas as pd

from .derive_decorator import wrap_derive
from .registrert import PRG_RANGES
from .registrert import raise_vg_utdprogram_outside_ranges
from nudb_use.metadata.nudb_config.map_get_dtypes import TYPE_MAPPINGS

BOOL_DTYPE = TYPE_MAPPINGS["pandas"]["BOOL_DTYPE_NAME"]
FULLFORTKODE = "8"


__all__ = [
    "gr_ergrunnskole_fullfort",
    "uh_erbachelor_fullfort",
    "uh_erdoktorgrad_fullfort",
    "uh_erhoyskolekandidat_fullfort",
    "uh_ermaster_fullfort",
    "vg_erstudiespess_fullfort",
    "vg_ervgo_fullfort",
    "vg_eryrkesfag_fullfort",
]


@wrap_derive
def gr_ergrunnskole_fullfort(  # noqa: DOC101,DOC103,DOC201,DOC203
    df: pd.DataFrame,
) -> pd.Series:
    """Derive gr_ergrunnskole_fullfort from nus2000, utd_fullfoertkode, uh_erutland."""
    return (
        (df["nus2000"].str[0] == "2")
        & (~df["uh_erutland"])
        & (df["utd_fullfoertkode"] == FULLFORTKODE)
    ).astype(BOOL_DTYPE)


@wrap_derive
def vg_ervgo_fullfort(  # noqa: DOC101,DOC103,DOC201,DOC203
    df: pd.DataFrame,
) -> pd.Series:
    """Derive vg_ervgo_fullfort from nus2000, utd_fullfoertkode, vg_kompetanse_nus and utd_aktivitet_start."""
    return (
        (df["nus2000"].str[0].isin(["4", "5"]))
        & (df["utd_fullfoertkode"] == FULLFORTKODE)
        & (
            (df["vg_kompetanse_nus"].isin(["1", "2", "3", "5"]))
            | (
                df["utd_aktivitet_start"]
                < datetime.datetime.strptime("2000-08-01", r"%Y-%m-%d")
            )  # Komp ikke utylt før 2000?
        )
    ).astype(BOOL_DTYPE)


@wrap_derive
def vg_erstudiespess_fullfort(  # noqa: DOC101,DOC103,DOC201,DOC203
    df: pd.DataFrame,
) -> pd.Series:
    """Derive vg_erstudiespess_fullfort from nus2000, vg_utdprogram, utd_fullfoertkode, vg_kompetanse_nus and utd_aktivitet_start."""
    raise_vg_utdprogram_outside_ranges(df["vg_utdprogram"])
    return (
        vg_ervgo_fullfort(df)["vg_ervgo_fullfort"]
        & (df["vg_utdprogram"].isin(PRG_RANGES["studiespess"]))
    ).astype(BOOL_DTYPE)


@wrap_derive
def vg_eryrkesfag_fullfort(  # noqa: DOC101,DOC103,DOC201,DOC203
    df: pd.DataFrame,
) -> pd.Series:
    """Derive vg_eryrkesfag_fullfort from nus2000, vg_utdprogram, utd_fullfoertkode, vg_kompetanse_nus and utd_aktivitet_start."""
    raise_vg_utdprogram_outside_ranges(df["vg_utdprogram"])
    return (
        vg_ervgo_fullfort(df)["vg_ervgo_fullfort"]
        & (df["vg_utdprogram"].isin(PRG_RANGES["yrkesfag"]))
    ).astype(BOOL_DTYPE)


@wrap_derive
def uh_erhoyskolekandidat_fullfort(  # noqa: DOC101,DOC103,DOC201,DOC203
    df: pd.DataFrame,
) -> pd.Series:
    """Derive vg_eryrkesfag_fullfort from nus2000, vg_utdprogram, utd_fullfoertkode, vg_kompetanse_nus and utd_aktivitet_start."""
    return (
        (df["nus2000"].str[0] == "6")
        & (df["utd_fullfoertkode"] == FULLFORTKODE)
        & (df["utd_klassetrinn"].astype("Int64").isin([15, 16]))
        & (
            ~df["uh_gruppering_nus"].isin(["01", "02"])
        )  # Ikke "Forberedende prøver", eller "Lavere nivås utdanning"
    ).astype(BOOL_DTYPE)


@wrap_derive
def uh_erbachelor_fullfort(  # noqa: DOC101,DOC103,DOC201,DOC203
    df: pd.DataFrame,
) -> pd.Series:
    """Derive uh_erbachelor_fullfort from uh_gruppering_nus, utd_fullfoertkode."""
    return (
        (df["uh_gruppering_nus"].str[3] == "B")
        & (df["utd_fullfoertkode"] == FULLFORTKODE)
    ).astype(BOOL_DTYPE)


@wrap_derive
def uh_ermaster_fullfort(  # noqa: DOC101,DOC103,DOC201,DOC203
    df: pd.DataFrame,
) -> pd.Series:
    """Derive uh_ermaster_fullfort from nus2000, utd_fullfoertkode."""
    return (
        (df["nus2000"].str[0] == "7") & (df["utd_fullfoertkode"] == FULLFORTKODE)
    ).astype(BOOL_DTYPE)


@wrap_derive
def uh_erdoktorgrad_fullfort(  # noqa: DOC101,DOC103,DOC201,DOC203
    df: pd.DataFrame,
) -> pd.Series:
    """Derive uh_erdoktorgrad_fullfort from nus2000, utd_fullfoertkode."""
    return (
        (df["nus2000"].str[0] == "8") & (df["utd_fullfoertkode"] == FULLFORTKODE)
    ).astype(BOOL_DTYPE)
