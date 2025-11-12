from typing import Callable
import pandas as pd
from pandas.io.formats.style import Styler

def _color_cell(code: bool) -> str:
    color = "green" if code == "OK" else "red"
    return f"color: white; background-color: {color}"

def empty_cols_in_time_colored(df: pd.DataFrame, time_col: str) -> Styler:
    return (df.groupby(time_col).agg(lambda x: "NA" if x.isna().all() else "OK").style.map(_color_cell))


def _grade_cell(spercent: float,
                scaling_fun: Callable = lambda x: x,
                colorscale: float = 0.83) -> str:
    ratio = float(spercent[0:len(spercent) - 1]) / 100
    
    percentage = 100 * scaling_fun(ratio)
    
    red   = (100 - percentage) * colorscale
    green = percentage * colorscale

    return f"color: white; background-color: rgb({red}%, {green}%, 0%)"

def grade_cell_by_time_col(df: pd.DataFrame, time_col) -> Styler:
    return (
        df.groupby(time_col)
            .agg(lambda x: str(100 - 100 * float(x.isna().sum() / len(x)))[0:5] + "%")
            .astype("str")
            .style
            .map(_grade_cell, scaling_fun = lambda x: x**4)
    )
