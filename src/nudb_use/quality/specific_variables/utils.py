"""Shared helpers for the variable-specific validation modules."""

import inspect

import pandas as pd

from nudb_use import logger
from nudb_use.exceptions.exception_classes import NudbQualityError


def get_column(df: pd.DataFrame, col: str) -> pd.Series | None:
    """Return a DataFrame column or None when it is missing."""
    return df[col] if col in df.columns else None


def add_err2list(errors: list, error: None | NudbQualityError) -> None:
    """Append a validation error to a list if it is not None."""
    if error is not None:
        errors.append(error)


def args_have_None(**kwargs: dict[str, pd.Series | None]) -> bool:
    """Log and return True if any keyword arguments are None."""
    FUNCTION_NAME = inspect.currentframe().f_back.f_code.co_name

    for key, value in kwargs.items():
        if value is None:
            logger.info(
                f"Terminating: `{FUNCTION_NAME}()`, Reason: `{key}` is `None` - maybe the needed columns are not in the dataset?"
            )
            return True

    # ugly side effect right here...
    logger.info(f"Args are OK, running `{FUNCTION_NAME}()`")

    return False
