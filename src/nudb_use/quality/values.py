"""Utilities for inspecting column fill rates and unexpected values."""

from collections.abc import Iterable
from collections.abc import Sequence

import pandas as pd


def get_fill_amount_per_column(df: pd.DataFrame) -> dict[str, float]:
    """Calculate the percentage of filled (non-null) values per column.

    Args:
        df: DataFrame whose columns should be summarized.

    Returns:
        dict[str, float]: Mapping of column name to percentage of filled cells.
    """
    return {col: (df[col].notna().sum() / len(df) * 100) for col in df.columns}


def values_not_in_column(
    col: pd.Series, values: Sequence[object] | object, raise_error: bool = False
) -> None | ValueError:
    """Check whether certain values appear inside a column.

    Args:
        col: Series to inspect.
        values: Allowed values; may be a single value or a list.
        raise_error: When True, raise ValueError immediately when matches occur.

    Returns:
        None | ValueError: None when the column is clean, otherwise a ValueError
        describing the unexpected values.

    Raises:
        ValueError: If forbidden values are found and `raise_error` is True.
    """
    if not (isinstance(values, Iterable) and not isinstance(values, (str, bytes))):
        values = [values]

    values_col_mask = col.isin(values)
    values_in_col = col[values_col_mask]

    if len(values_in_col):
        err_msg = (
            "Values in col that shouldnt be there: "
            f"{values_in_col.unique()}, amount of rows: {len(values_in_col)}"
        )
        if raise_error:
            raise ValueError(err_msg)
        return ValueError(err_msg)
    return None
