"""Shared helpers for the variable-specific validation modules."""

import inspect
from typing import cast

import pandas as pd

from nudb_use.exceptions.exception_classes import NudbQualityError
from nudb_use.nudb_logger import logger


def get_column(df: pd.DataFrame, col: str) -> pd.Series | None:
    """Return a DataFrame column or None when it is missing."""
    return df[col] if col in df.columns else None


def add_err2list(
    errors: list[NudbQualityError], error: None | NudbQualityError
) -> None:
    """Append a validation error to a list if it is not None."""
    if error is not None:
        errors.append(error)


def require_series_present(
    **series_by_name: pd.Series | None,
) -> dict[str, pd.Series] | None:
    """Ensure required pandas Series exist before continuing a validation step."""
    caller = inspect.currentframe()
    caller_frame = caller.f_back if caller else None
    function_name = caller_frame.f_code.co_name if caller_frame else "UNKNOWN"

    for name, series in series_by_name.items():
        if series is None:
            logger.info(
                f"Terminating: `{function_name}()`, Reason: `{name}` is `None` - maybe the needed columns are not in the dataset?"
            )
            return None

    logger.info(f"Args are OK, running `{function_name}()`")
    # mypy does not narrow automatically when dict values are passed through **,
    # so we build a new mapping that is explicitly typed as pd.Series.
    result: dict[str, pd.Series] = {}
    for name, series in series_by_name.items():
        result[name] = cast(pd.Series, series)
    return result
