"""Utilities for reorganizing and trimming NUDB datasets."""

import pandas as pd


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
