"""Validations for the SN07 classification variable."""

import pandas as pd

from nudb_use.exceptions.exception_classes import NudbQualityError
from nudb_use.nudb_logger import LoggerStack
from nudb_use.nudb_logger import logger

from .utils import add_err2list
from .utils import get_column
from .utils import require_series_present


def check_sn07(df: pd.DataFrame, **kwargs: object) -> list[NudbQualityError]:
    """Execute SN07-specific checks and return collected errors.

    Args:
        df: DataFrame containing SN07 codes.
        **kwargs: Placeholder for future configuration. Passed in from parent function.

    Returns:
        list[NudbQualityError]: Errors describing invalid SN07 codes.
    """
    with LoggerStack("Validating for specific variable: sn07"):
        sn07 = get_column(df, col="sn07")

        errors: list[NudbQualityError] = []
        add_err2list(errors, subcheck_sn07_bad_value(sn07))

        return errors


def subcheck_sn07_bad_value(sn07: pd.Series | None) -> NudbQualityError | None:
    """Detect disallowed SN07 codes.

    Args:
        sn07: Series containing SN07 codes to inspect.

    Returns:
        NudbQualityError | None: Error when forbidden codes are present, else None.
    """
    validated = require_series_present(sn07_col=sn07)
    if validated is None:
        return None
    sn07 = validated["sn07_col"]

    # Find the unique codes in the column
    sn07_unique = [v for v in sn07.unique() if not pd.isna(v) and v]
    if not sn07_unique:
        logger.debug("No values found in sn07-column, exiting check early.")
        return None

    wrong_vals = {"992580": "Ukjent næring i Utlandet?"}
    wrong_used = {k: v for k, v in wrong_vals.items() if k in sn07_unique}

    if not wrong_used:
        return None

    err_msg = f"Weird SN07 values used in data: {wrong_used}"
    logger.warning(err_msg)
    return NudbQualityError(err_msg)
