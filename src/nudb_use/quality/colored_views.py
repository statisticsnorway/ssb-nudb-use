"""Style helpers that colorize quality summary tables."""

from collections.abc import Callable

import pandas as pd
from pandas.io.formats.style import Styler


def _color_cell(code: bool) -> str:
    """Map OK/NA cells to red/green background colors.

    Args:
        code: Flag indicating whether the value is acceptable.

    Returns:
        str: CSS style string.
    """
    color = "green" if code == "OK" else "red"
    return f"color: white; background-color: {color}"


def empty_cols_in_time_colored(df: pd.DataFrame, time_col: str) -> Styler:
    """Highlight columns that are empty for entire periods.

    Args:
        df: DataFrame to summarize.
        time_col: Column to group by when calculating emptiness.

    Returns:
        Styler: Styled output with red/green highlights.
    """
    return (
        df.groupby(time_col)
        .agg(lambda x: "NA" if x.isna().all() else "OK")
        .style.map(_color_cell)
    )


def _grade_cell(
    spercent: float, scaling_fun: Callable = lambda x: x, colorscale: float = 0.83
) -> str:
    """Shade a cell based on a string percentage.

    Args:
        spercent: Percentage string, e.g. '85%'.
        scaling_fun: Callable applied to the ratio before coloring.
        colorscale: Scalar used to scale RGB intensities.

    Returns:
        str: CSS style string.
    """
    ratio = float(spercent[0 : len(spercent) - 1]) / 100

    percentage = 100 * scaling_fun(ratio)

    red = (100 - percentage) * colorscale
    green = percentage * colorscale

    return f"color: white; background-color: rgb({red}%, {green}%, 0%)"


def grade_cell_by_time_col(df: pd.DataFrame, time_col: str) -> Styler:
    """Colorize per-period completeness percentages.

    Args:
        df: DataFrame to summarize.
        time_col: Column representing the grouping period.

    Returns:
        Styler: Styled completeness summary for display.
    """
    return (
        df.groupby(time_col)
        .agg(lambda x: str(100 - 100 * float(x.isna().sum() / len(x)))[0:5] + "%")
        .astype("str")
        .style.map(_grade_cell, scaling_fun=lambda x: x**4)
    )
