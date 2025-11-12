from typing import Any
import pandas as pd
from collections.abc import Iterable
from nudb_use import logger, LoggerStack

def filled_value_to_threshold(col: pd.Series, value: Any, threshold_lower: float, raise_error: bool = True) -> ValueError | None:
    """Check the fill rate of a value in a column is above a threshold. 

    Args:
        col: Name of column to check.
        value: Value to check for. 
        threshold_lower: Lower threshold of percentage fill rate for column. 
        raise_error: If True, raises an exception group on values below the threshold;
            otherwise, only logs warnings.
    
    Returns:
        error[ValueError]: Returns a ValueError instance if the threshold is not met and
            `raise_error` is False; otherwise, returns None.
        
    Raises:
        ValueError: If the percentage of matching values is below the threshold and `raise_error` is True.
    """
    if isinstance(value, Iterable) and not isinstance(value, (str, bytes)):
        pass
    else:
        value = [value]
    
    percent = ((col.isin(value)).sum() / len(col)) * 100
    error: None | ValueError = None
    if threshold_lower > percent:
        error = ValueError(f"mrk_dl {percent} is below the threshold of {threshold_lower}%")
        if raise_error:
            raise error
    return error


def non_empty_to_threshold(col: pd.Series, threshold_lower: float, raise_error: bool = True) -> ValueError | None:
    """Check if the percentage of empty values in a dataframe is above a threshold. 

    Args: 
        col: Name of column to check. 
        threshold_lower: Lower threshold of percentage empty columns. 
        raise_error: If True, raises an exception group on empty values below the threshold;
            otherwise, only logs warnings.

    Returns:
        error[ValueError]: Returns a ValueError instance if the threshold is not met and
            `raise_error` is False; otherwise, returns None.
        
    Raises:
        ValueError: If the percentage of empty values is below the threshold and `raise_error` is True.
    """
    percent_empty = ((col.isna()).sum() / len(col)) * 100
    error: None | ValueError = None
    if threshold_lower > percent_empty:
        error = ValueError(f"mrk_dl {percent_empty} is below the threshold of {threshold_lower}%")
        if raise_error:
            raise error
    return error
