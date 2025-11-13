from collections.abc import Iterable
from typing import Any

import pandas as pd


def get_fill_amount_per_column(df: pd.DataFrame) -> dict[str, float]:
    """Get the percentage of filled cells (non-NA) in each column, and store in a dict.

    Args:
        df:

    Returns:

    """
    return {col: (df[col].notna().sum() / len(df) * 100) for col in df.columns}


def values_not_in_column(
    col: pd.Series, values: list[Any] | Any, raise_error: bool = False
) -> None | ValueError:
    """Check for the presence of given values in a column.

    Args:
        col: Column to check.
        values: List of values to check for in the column.
        raise_error: If True, raises an exception group on validation errors;
                     otherwise, only logs warnings.

    Returns:
        err[ValueError]: List of ValueErrors found during the check, empty if None.

    Raises:
        ValueError: If any non-declared values are found in the column and 'raise_error' is True.
    """
    if isinstance(values, Iterable) and not isinstance(values, (str, bytes)):
        pass
    else:
        values = [values]

    values_col_mask = col.isin(values)
    values_in_col = col[values_col_mask]

    if len(values_in_col):
        err = ValueError(
            f"Values in col that shouldnt be there: {values_in_col.unique()}, amount of rows: {len(values_in_col)}"
        )
        if raise_error:
            raise err
        return err
    return None
