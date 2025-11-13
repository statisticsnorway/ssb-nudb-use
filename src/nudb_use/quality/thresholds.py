"""Threshold helpers for completeness and fill-rate validations."""

from collections.abc import Iterable
from nudb_use.exceptions.exception_classes import NudbQualityError

import pandas as pd


def filled_value_to_threshold(
    col: pd.Series,
    value: Iterable[object] | object,
    threshold_lower: float,
    raise_error: bool = True,
) -> ValueError | None:
    """Ensure the proportion of specific values stays above a threshold.

    Args:
        col: Series to inspect.
        value: Single value or iterable of values that must meet the threshold.
        threshold_lower: Minimum allowed percentage of matching values.
        raise_error: When True, raise ValueError if the threshold is not met.

    Returns:
        ValueError: Error describing the shortage when the threshold is not met
            and `raise_error` is False.
        None: Returned when the column satisfies the threshold.

    Raises:
        ValueError: If the percentage of matching values is below the threshold
            while `raise_error` is True.
    """
    if isinstance(value, Iterable) and not isinstance(value, (str, bytes)):
        pass
    else:
        value = [value]

    percent = ((col.isin(value)).sum() / len(col)) * 100
    if threshold_lower > percent:
        err_msg = f"mrk_dl {percent} is below the threshold of {threshold_lower}%"
        if raise_error:
            raise NudbQualityError(err_msg)
        return NudbQualityError(err_msg)
    return None


def non_empty_to_threshold(
    col: pd.Series, threshold_lower: float, raise_error: bool = True
) -> ValueError | None:
    """Ensure the proportion of non-empty values stays above a threshold.

    Args:
        col: Series to inspect.
        threshold_lower: Minimum allowed percentage of non-empty values.
        raise_error: When True, raise ValueError if the threshold is not met.

    Returns:
        ValueError: Error describing the shortage when the threshold is not met
            and `raise_error` is False.
        None: Returned when the column satisfies the threshold.

    Raises:
        ValueError: If the percentage of non-empty values is below the threshold
            while `raise_error` is True.
    """
    percent_empty = ((col.isna()).sum() / len(col)) * 100
    percent_filled = 100 - percent_empty
    if threshold_lower > percent_filled:
        err_msg = (
            f"mrk_dl {percent_filled} is below the threshold of {threshold_lower}%"
        )
        if raise_error:
            raise NudbQualityError(err_msg)
        return NudbQualityError(err_msg)
    return None
