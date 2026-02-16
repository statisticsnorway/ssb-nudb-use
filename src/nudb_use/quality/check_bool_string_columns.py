"""Checks for string columns containing boolean-like literal values."""

import pandas as pd
from pandas.api.types import is_string_dtype

from nudb_use.exceptions.exception_classes import NudbQualityError
from nudb_use.exceptions.groups import raise_exception_group
from nudb_use.exceptions.groups import warn_exception_group
from nudb_use.nudb_logger import LoggerStack

_BOOL_LITERALS = {"True", "False"}


def check_bool_string_columns(
    df: pd.DataFrame, raise_errors: bool = True
) -> list[NudbQualityError]:
    """Detect string columns that contain literal boolean values.

    Args:
        df: DataFrame to inspect.
        raise_errors: When True, raise grouped errors if violations are found.

    Returns:
        list[NudbQualityError]: Errors describing columns with boolean-like
        string literals, or an empty list when none are found.
    """
    with LoggerStack(
        "Checking for string columns with literal boolean values (True/False)."
    ):
        errors: list[NudbQualityError] = []
        for col in df.columns:
            series = df[col]
            if not is_string_dtype(series):
                continue
            string_mask = series.map(lambda value: isinstance(value, str))
            if not bool(string_mask.any()):
                continue
            string_values = series[string_mask]
            normalized = string_values.str.strip()
            bool_mask = normalized.isin(_BOOL_LITERALS)
            if not bool(bool_mask.any()):
                continue
            count = int(bool_mask.sum())
            sample_values = normalized[bool_mask].dropna().unique()[:5]
            sample = ", ".join(sample_values) if len(sample_values) else "True/False"
            err_msg = (
                f"Column {col} contains {count} literal boolean strings ({sample}). "
                "This may indicate a bool column converted to string."
            )
            errors.append(NudbQualityError(err_msg))

        if raise_errors:
            raise_exception_group(errors)
        else:
            warn_exception_group(errors)

        return errors
