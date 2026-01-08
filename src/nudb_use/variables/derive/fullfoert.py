import pandas as pd

from .derive_decorator import wrap_derive
from .registrert import PRG_RANGES, raise_vg_utdprogram_outside_ranges

import datetime


FULLFORTKODE = "8"


@wrap_derive
def gr_ergrunnskole_fullfort(  # noqa: DOC101,DOC103,DOC201,DOC203
    df: pd.DataFrame,
) -> pd.Series:
    """Derive gr_ergrunnskole_fullfort from nus2000, utd_fullfoertkode, uh_erutland."""
    return (
                (df["nus2000"].str[0] == "2") 
                & (~df["uh_erutland"])
                & (df["utd_fullfoertkode"] == FULLFORTKODE)
            ).astype("bool[pyarrow]")

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
                    | (df["utd_aktivitet_start"] < datetime.datetime.strftime("2000-08-01"))  # Komp ikke utylt før 2000?
                )
        ).astype("bool[pyarrow]")


@wrap_derive
def vg_erstudiespess_fullfort(  # noqa: DOC101,DOC103,DOC201,DOC203
    df: pd.DataFrame,
) -> pd.Series:
    """Derive vg_erstudiespess_fullfort from nus2000, vg_utdprogram, utd_fullfoertkode, vg_kompetanse_nus and utd_aktivitet_start."""
    raise_vg_utdprogram_outside_ranges(df["vg_utdprogram"])
    return (vg_ervgo_fullfort(df) & (df["vg_utdprogram"].isin(PRG_RANGES["studiespess"]))).astype("bool[pyarrow]")

@wrap_derive
def vg_eryrkesfag_fullfort(  # noqa: DOC101,DOC103,DOC201,DOC203
    df: pd.DataFrame,
) -> pd.Series:
    """Derive vg_eryrkesfag_fullfort from nus2000, vg_utdprogram, utd_fullfoertkode, vg_kompetanse_nus and utd_aktivitet_start."""
    raise_vg_utdprogram_outside_ranges(df["vg_utdprogram"])
    return (vg_ervgo_fullfort(df) & (df["vg_utdprogram"].isin(PRG_RANGES["yrkesfag"]))).astype("bool[pyarrow]")



@wrap_derive
def uh_erhoyskolekandidat_fullfort(  # noqa: DOC101,DOC103,DOC201,DOC203
    df: pd.DataFrame,
) -> pd.Series:
    """Derive vg_eryrkesfag_fullfort from nus2000, vg_utdprogram, utd_fullfoertkode, vg_kompetanse_nus and utd_aktivitet_start."""
    return (
        (df["nus2000"].str[0] == "6")
        & (df["utd_fullfoertkode"] == FULLFORTKODE)
        & (df["utd_klassetrinn"].astype("Int64").isin([15, 16]))
        & (~df["uh_gruppering_nus"].isin(["01", "02"]))  # Ikke "Forberedende prøver", eller "Lavere nivås utdanning"
    ).astype("bool[pyarrow]")

# Todo
# uh_erbachelor_fullfort
# uh_ermaster_fullfort
# uh_erdoktorgrad_fullfort