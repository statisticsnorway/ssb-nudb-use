import pandas as pd

from nudb_use.metadata.nudb_config.map_get_dtypes import BOOL_DTYPE_NAME
from nudb_use.metadata.nudb_config.map_get_dtypes import DTYPE_MAPPINGS
from nudb_use.nudb_logger import logger

from .derive_decorator import wrap_derive

BOOL_DTYPE = DTYPE_MAPPINGS["pandas"][BOOL_DTYPE_NAME]

__all__ = [
    "gr_ergrunnskole_registrering",
    "uh_erbachelor_registrering",
    "uh_erhoeyereutd_registrering",
    "uh_ermaster_registrering",
    "vg_erstudiespess_registrering",
    "vg_ervgo_registrering",
    "vg_eryrkesfag_registrering",
]


# Would be nice if these were complete in klass instead - the variant on nus is not complete?
PRG_RANGES_RANGES: dict[str, list[range]] = {
    "studiespess": [
        range(1, 2),  # this is not a mistake -> [1, 2) -> [1]
        range(21, 24),
        range(60, 65),
    ],
    "yrkesfag": [
        range(3, 20),
        range(30, 43),
        range(50, 51),  # this is not a mistake -> [50, 51) -> [50]
        range(70, 84),
        range(98, 100),
    ],
}
PRG_RANGES: dict[str, list[str]] = {}
for k, v in PRG_RANGES_RANGES.items():
    PRG_RANGES[k] = [y for rng in v for y in [str(n).zfill(2) for n in rng]]


@wrap_derive
def gr_ergrunnskole_registrering(  # noqa: DOC101,DOC103,DOC201,DOC203
    df: pd.DataFrame,
) -> pd.Series:
    """Derive gr_ergrunnskole_registrering from nus2000 and utland, as a boolean filter for registrations on gr-level."""
    bool_mask: pd.Series = (
        (df["nus2000"].str[0] == "2") & (~df["utd_erutland"])
    ).astype(BOOL_DTYPE)
    return bool_mask


@wrap_derive
def vg_ervgo_registrering(  # noqa: DOC101,DOC103,DOC201,DOC203
    df: pd.DataFrame,
) -> pd.Series:
    """Derive vg_ervgo_registrering from nus2000, as a boolean filter for registrations on vg-level."""
    bool_mask: pd.Series = df["nus2000"].str[0].isin(["3", "4"]).astype(BOOL_DTYPE)
    return bool_mask


def raise_vg_utdprogram_outside_ranges(vg_utdprogram: pd.Series) -> None:
    """Raise an error if the vg_utdprogram are outside the defined ranges.

    Args:
        vg_utdprogram: The column containing the the vg_utdprogram.

    Raises:
        ValueError: If the column contains values not defined in the ranges.
    """
    values_outside_ranges = [
        val
        for val in vg_utdprogram[vg_utdprogram.notna()].unique()
        if val not in [v for x in PRG_RANGES.values() for v in x]
    ]
    if values_outside_ranges:
        err_msg = f"Found vg_utdprogram values outside valid codelist, data or code should be fixed: {values_outside_ranges}"
        raise ValueError(err_msg)
    return None


@wrap_derive
def vg_erstudiespess_registrering(  # noqa: DOC101,DOC103,DOC201,DOC203
    df: pd.DataFrame,
) -> pd.Series:
    """Derive vg_erstudiespess_registrering from nus2000 and vg_utdprogram, as a boolean filter."""
    raise_vg_utdprogram_outside_ranges(df["vg_utdprogram"])
    bool_mask: pd.Series = (
        (df["nus2000"].str[0].isin(["3", "4"]))
        & (df["vg_utdprogram"].isin(PRG_RANGES["studiespess"]))
    ).astype(BOOL_DTYPE)
    return bool_mask


@wrap_derive
def vg_eryrkesfag_registrering(  # noqa: DOC101,DOC103,DOC201,DOC203
    df: pd.DataFrame,
) -> pd.Series:
    """Derive vg_eryrkesfag_registrering from nus2000 and vg_utdprogram, as a boolean filter."""
    raise_vg_utdprogram_outside_ranges(df["vg_utdprogram"])
    bool_mask: pd.Series = (
        (df["nus2000"].str[0].isin(["3", "4"]))
        & (df["vg_utdprogram"].isin(PRG_RANGES["yrkesfag"]))
    ).astype(BOOL_DTYPE)
    return bool_mask


@wrap_derive
def uh_erhoeyereutd_registrering(  # noqa: DOC101,DOC103,DOC201,DOC203
    df: pd.DataFrame,
) -> pd.Series:
    """Derive uh_erhoeyereutd_registrering from nus2000 as a boolean filter."""
    bool_mask: pd.Series = (df["nus2000"].str[0].isin(["6", "7", "8"])).astype(
        BOOL_DTYPE
    )
    return bool_mask


@wrap_derive
def uh_erbachelor_registrering(  # noqa: DOC101,DOC103,DOC201,DOC203
    df: pd.DataFrame,
) -> pd.Series:
    """Derive uh_erbachelor_registrering from nus2000 as a boolean filter."""
    bool_mask: pd.Series = (df["uh_gradmerke_nus"] == "B").astype(BOOL_DTYPE)
    logger.info(type(bool_mask))
    logger.info(bool_mask)
    return bool_mask


@wrap_derive
def uh_ermaster_registrering(  # noqa: DOC101,DOC103,DOC201,DOC203
    df: pd.DataFrame,
) -> pd.Series:
    """Derive uh_erbachelor_registrering from nus2000 as a boolean filter."""
    bool_mask: pd.Series = (df["nus2000"].str[0] == "7").astype(BOOL_DTYPE)
    return bool_mask
