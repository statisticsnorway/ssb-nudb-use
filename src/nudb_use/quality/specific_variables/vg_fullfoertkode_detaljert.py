"""Validations for the `vg_fullfoertkode_detaljert` variable."""

import pandas as pd

from nudb_use.exceptions.exception_classes import NudbQualityError
from nudb_use.nudb_logger import LoggerStack
from nudb_use.nudb_logger import logger

from .utils import add_err2list
from .utils import get_column
from .utils import require_series_present


def check_vg_fullfoertkode_detaljert(
    df: pd.DataFrame, **kwargs: object
) -> list[NudbQualityError]:
    """Run all vg_fullfoertkode_detaljert-specific checks on the provided DataFrame.

    Args:
        df: DataFrame containing vg_fullfoertkode_detaljert and supporting columns.
        **kwargs: Additional keyword arguments for future compatibility.

    Returns:
        list[NudbQualityError]: Errors reported by the vg_fullfoertkode_detaljert checks.
    """
    with LoggerStack("Validating specific variable: vg_fullfoertkode_detaljert"):
        utd_utdanningstype = get_column(df, col="utd_utdanningstype")
        vg_fullfoertkode_detaljert = get_column(df, col="vg_fullfoertkode_detaljert")

        errors: list[NudbQualityError] = []
        add_err2list(
            errors,
            subcheck_vg_fullfoertkode_detaljert_utd_211(
                utd_utdanningstype, vg_fullfoertkode_detaljert
            ),
        )

        return errors


def subcheck_vg_fullfoertkode_detaljert_utd_211(
    utd_utdanningstype: pd.Series | None, vg_fullfoertkode_detaljert: pd.Series | None
) -> NudbQualityError | None:
    """Ensure vg_fullfoertkode_detaljert is filled only for utd_utdanningstype 211, 212, 220 and 610.

    Args:
        utd_utdanningstype: Series containing the utdanningstype codes.
        vg_fullfoertkode_detaljert: Series containing gro_elevstatus values.

    Returns:
        NudbQualityError | None: Error when invalid combinations exist, else None.
    """
    validated = require_series_present(
        utd_utdanningstype=utd_utdanningstype,
        vg_fullfoertkode_detaljert=vg_fullfoertkode_detaljert,
    )
    if validated is None:
        return None
    utd_utdanningstype = validated["utd_utdanningstype"]
    vg_fullfoertkode_detaljert = validated["vg_fullfoertkode_detaljert"]

    outside_vg = ~(
        utd_utdanningstype.isin(
            (
                "211",
                "212",
                "220",
                "610",
            )
        )
    )
    has_fulldetj = vg_fullfoertkode_detaljert.notna()
    wrong = outside_vg & has_fulldetj

    if not wrong.sum():
        return None

    err_msg = "Where `utd_utdanningstype` is something other than 211, 212, 220 and 610 (not vg), `vg_fullfoertkode_detaljert` should be empty."
    logger.warning(err_msg)
    return NudbQualityError(err_msg)
