"""Utilities for reorganizing and trimming NUDB datasets."""

import pandas as pd


def sort_all_values(
    df: pd.DataFrame, priority_cols: list[str] | None = None
) -> pd.DataFrame:
    """Sort the dataset semi-deterministically, by using all columns, but prioritizing some.

    Notes:
        - If the people analyzing our data do not sort on all the columns…
        - And they use a semi-random “keep first of duplicates strategy”…
        - They will be dependant on the order of values we are sending them to reproduce the same aggregations across versions…
        - So providing a semi-deterministic sort order of values might be a good thing?
        - Still, people removing duplicates dependent on a semi-random strategy, should strive to make it less random…

    Args:
        df: The dataframe to be sorted.
        priority_cols: A list of columns to weigh first in sorting.

    Returns:
        pd.DataFrame: A pandas dataframe sorted.

    Raises:
        KeyError: If some of the priority_cols are not in the dataframe sent in.
    """
    if priority_cols is None:
        priority_cols_list: list[str] = [
            "utd_skoleaar_start",
            "nus2000",
            "utd_skolekom",
        ]
    else:
        priority_cols_list = priority_cols

    if not all(col in df.columns for col in priority_cols_list):
        miss_cols = ", ".join(c for c in priority_cols_list if c not in df.columns)
        raise KeyError(
            f"Columns prioritized for sorting not in the dataset (send on your own): {miss_cols}"
        )

    cols_order = priority_cols_list + sorted(
        c for c in df.columns if c not in priority_cols_list
    )
    return df.sort_values(cols_order)


def move_col_after_col(
    df: pd.DataFrame, col_anchor: str, col_move_after: str
) -> pd.DataFrame:
    """Move a specified column in a DataFrame to immediately follow another specified column.

    Args:
        df: Input pandas DataFrame.
        col_anchor: Name of the column after which the specified column will be moved.
        col_move_after: Name of the column to move.

    Returns:
        pd.DataFrame: New DataFrame with the specified column moved to follow the anchor column.
    """
    col_move_content = df[col_move_after]
    df = df.drop(columns=col_move_after)
    ind = list(df.columns).index(col_anchor)  # Important to do this after the drop...
    df.insert(ind + 1, col_move_after, col_move_content)
    return df


def move_content_from_col_to(
    df: pd.DataFrame, from_col: str, to_col: str
) -> pd.DataFrame:
    """Fill empty values (NA) in one column with values from another column.

    Args:
        df: DataFrame
        from_col: Column where information is taken.
        to_col: Column where information is moved to.

    Returns:
        pd.DataFrame: DataFrame with values filled out.
    """
    if from_col in df.columns:
        df[to_col] = df[to_col].fillna(df[from_col])
        return df.drop(columns=[from_col])
    return df
