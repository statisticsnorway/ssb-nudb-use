"""Validations for the `gr_grunnskolepoeng` variable."""

import pandas as pd

from nudb_use.exceptions.exception_classes import NudbQualityError
from nudb_use.nudb_logger import LoggerStack
from nudb_use.nudb_logger import logger

from .utils import add_err2list
from .utils import get_column
from .utils import require_series_present


def check_grunnskolepoeng(df: pd.DataFrame, **kwargs: object) -> list[NudbQualityError]:
    """Run grunnskolepoeng validations on the provided dataset.

    Args:
        df: DataFrame containing the grunnskolepoeng column.
        **kwargs: Currently unused keyword arguments. Passed in from parent function.

    Returns:
        list[NudbQualityError]: Errors produced by the sub-checks.
    """
    with LoggerStack("Validating specific variable: gr_grunnskolepoeng"):
        grunnskolepoeng = get_column(df, "gr_grunnskolepoeng")

        errors: list[NudbQualityError] = []

        add_err2list(errors, subcheck_grunnskolepoeng_maxval(grunnskolepoeng))

        return errors


def subcheck_grunnskolepoeng_maxval(
    grunnskolepoeng: pd.Series | None, max_poeng: float = 70.0
) -> NudbQualityError | None:
    """Verify that grunnskolepoeng values stay within the allowed maximum.

    Args:
        grunnskolepoeng: Series containing grunnskolepoeng values.
        max_poeng: Maximum allowed points.

    Returns:
        NudbQualityError | None: Error when values exceed the maximum, else None.
    """
    validated = require_series_present(grunnskolepoeng=grunnskolepoeng)
    if validated is None:
        return None
    grunnskolepoeng = validated["grunnskolepoeng"]

    ok = (grunnskolepoeng <= max_poeng).all()
    if not ok:
        err_msg = f"Found values in grunnskolepoeng larger than {max_poeng}!"
        logger.warning(err_msg)
        return NudbQualityError(err_msg)

    return None
