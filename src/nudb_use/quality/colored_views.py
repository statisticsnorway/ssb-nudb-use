"""Style helpers that colorize quality summary tables."""

from collections.abc import Callable
from typing import Any
from typing import Literal

import pandas as pd
from pandas.io.formats.style import Styler


def _color_cell(code: object) -> str:
    """Map OK/NA cells to red/green background colors.

    Args:
        code: Flag indicating whether the value is acceptable.

    Returns:
        str: CSS style string.
    """
    value = str(code)
    color = "green" if value == "OK" else "red"
    return f"color: white; background-color: {color}"


def empty_cols_in_time_colored(df: pd.DataFrame, time_col: str) -> Styler:
    """Highlight columns that are empty for entire periods.

    Args:
        df: DataFrame to summarize.
        time_col: Column to group by when calculating emptiness.

    Returns:
        Styler: Styled output with red/green highlights.
    """

    def na_ok(x: pd.Series[Any]) -> Literal["NA"] | Literal["OK"]:
        if x.isna().all():
            return "NA"
        return "OK"

    return df.groupby(time_col).agg(na_ok).style.map(_color_cell)


def _grade_cell(
    spercent: object,
    scaling_fun: Callable[[float], float] | None = None,
    colorscale: float = 0.83,
) -> str:
    """Shade a cell based on a string percentage.

    Args:
        spercent: Percentage string, e.g. '85%'.
        scaling_fun: Callable applied to the ratio before coloring.
        colorscale: Scalar used to scale RGB intensities.

    Returns:
        str: CSS style string.
    """
    transform = scaling_fun if scaling_fun is not None else (lambda value: value)
    ratio = float(str(spercent).rstrip("%")) / 100
    percentage = 100 * transform(ratio)

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

    def na_percent(x: pd.Series[int] | pd.Series[float]) -> str:
        return f"{(100 - 100 * float(x.isna().sum() / len(x))):0.2f}%"

    styled: Styler = (
        df.groupby(time_col)
        .agg(na_percent)
        .astype("string[pyarrow]")
        .style.map(_grade_cell, scaling_fun=lambda value: value**4)
    )
    return styled
