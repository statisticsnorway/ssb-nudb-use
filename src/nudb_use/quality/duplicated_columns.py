"""Checks for duplicated DataFrame columns and reports them as errors."""

import pandas as pd

from nudb_use.exceptions.exception_classes import NudbQualityError
from nudb_use.nudb_logger import LoggerStack
from nudb_use.nudb_logger import logger
from nudb_use.variables.var_utils.duped_columns import find_duplicated_columns


def check_duplicated_columns(df: pd.DataFrame) -> list[NudbQualityError]:
    """Return quality errors for duplicated columns in a DataFrame.

    Args:
        df: DataFrame to inspect.

    Returns:
        list[NudbQualityError]: Errors summarizing each duplicated column.
    """
    with LoggerStack(
        "Checking for duplicated columns in the dataset. (Would stop you from storing the file)."
    ):
        duped_columns = find_duplicated_columns(df)
        errors: list[NudbQualityError] = []
        for duped_col in duped_columns:
            err_msg = f"You have duplicated columns in your column: {duped_col}"
            logger.error(err_msg)
            errors.append(NudbQualityError(err_msg))
        if not duped_columns:
            logger.info("No duplicated columns found.")

        return errors
