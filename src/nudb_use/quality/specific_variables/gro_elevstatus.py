"""Validations for the `gro_elevstatus` variable."""

import pandas as pd

from nudb_use.exceptions.exception_classes import NudbQualityError
from nudb_use.nudb_logger import LoggerStack
from nudb_use.nudb_logger import logger

from .utils import add_err2list
from .utils import get_column
from .utils import require_series_present


def check_gro_elevstatus(df: pd.DataFrame, **kwargs: object) -> list[NudbQualityError]:
    """Run all gro_elevstatus-specific checks on the provided DataFrame.

    Args:
        df: DataFrame containing gro_elevstatus and supporting columns.
        **kwargs: Additional keyword arguments for future compatibility.

    Returns:
        list[NudbQualityError]: Errors reported by the gro_elevstatus checks.
    """
    with LoggerStack("Validating specific variable: gro_elevstatus"):
        utd_utdanningstype = get_column(df, col="utd_utdanningstype")
        gro_elevstatus = get_column(df, col="gro_elevstatus")

        errors: list[NudbQualityError] = []
        add_err2list(
            errors, subcheck_elevstatus_utd_211(utd_utdanningstype, gro_elevstatus)
        )

        return errors


def subcheck_elevstatus_utd_211(
    utd_utdanningstype: pd.Series | None, gro_elevstatus: pd.Series | None
) -> NudbQualityError | None:
    """Ensure gro_elevstatus is 'E' whenever utd_utdanningstype equals 211.

    Args:
        utd_utdanningstype: Series containing the utdanningstype codes.
        gro_elevstatus: Series containing gro_elevstatus values.

    Returns:
        NudbQualityError | None: Error when invalid combinations exist, else None.
    """
    validated = require_series_present(
        utd_utdanningstype=utd_utdanningstype, gro_elevstatus=gro_elevstatus
    )
    if validated is None:
        return None
    utd_utdanningstype = validated["utd_utdanningstype"]
    gro_elevstatus = validated["gro_elevstatus"]

    wrong = (utd_utdanningstype == "211") & (gro_elevstatus == "M")

    if not wrong.sum():
        return None

    err_msg = "Where `utd_utdanningstype`is 211, `gro_elevstatus` should be E, not M."
    logger.warning(err_msg)
    return NudbQualityError(err_msg)
