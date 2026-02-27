"""Checks for string columns containing boolean-like literal values."""

import pandas as pd

from nudb_use.exceptions.exception_classes import NudbQualityError
from nudb_use.exceptions.groups import raise_exception_group
from nudb_use.exceptions.groups import warn_exception_group
from nudb_use.nudb_logger import LoggerStack

_BOOL_LITERALS = {"True", "False"}
_SKIP_COLUMNS = ["snr", "fnr", "utd_orgnr", "bof_orgnrbed"]


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
        for col in df.select_dtypes(["object", "string", "string[pyarrow]"]).columns:
            if col in _SKIP_COLUMNS:
                continue
            count = pd.Series(df[col].unique()).isin(_BOOL_LITERALS).sum()
            if count:
                err_msg = (
                    f"Column {col} contains {count} booleans encoded as strings. "
                    "This may indicate a bool column converted to string by accident."
                )
                errors.append(NudbQualityError(err_msg))

        if raise_errors and errors:
            raise_exception_group(errors)
        elif errors:
            warn_exception_group(errors)

        return errors
