"""Helpers for finding duplicated DataFrame columns."""

from collections import Counter

import pandas as pd


def find_duplicated_columns(df: pd.DataFrame) -> list[str]:
    """Return column names that occur more than once in the DataFrame.

    Args:
        df: DataFrame whose columns should be inspected for duplicates.

    Returns:
        list[str]: Column names that appear more than once, or an empty list
        when all columns are unique.
    """
    counts = Counter(list(df.columns))
    return [item for item, count in counts.items() if count > 1]
