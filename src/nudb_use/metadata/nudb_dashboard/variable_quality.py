import pandas as pd
import numpy as np
from typing import Set, List

def calculate_fill_rate(series: pd.Series) -> float:
    """Calculates the fill rate of a pandas Series.

    Args:
        series: The pandas Series to calculate the fill rate for.

    Returns:
        The fill rate as a float between 0 and 1.
    """
    return 1 - (series.isnull().sum() / len(series))

def calculate_missing_ratios(series: pd.Series, missing_values: list) -> dict:
    """Calculates the ratio of missing/nan/filled values in a pandas Series.

    Args:
        series: The pandas Series to calculate the ratios for.
        missing_values: A list of values to be considered as missing.

    Returns:
        A dictionary with the ratios for missing, nan, and filled values.
    """
    total_count = len(series)
    nan_count = series.isnull().sum()
    missing_count = series.isin(missing_values).sum()
    filled_count = total_count - nan_count - missing_count
    
    return {
        "missing_ratio": missing_count / total_count if total_count > 0 else 0,
        "nan_ratio": nan_count / total_count if total_count > 0 else 0,
        "filled_ratio": filled_count / total_count if total_count > 0 else 0,
    }

def calculate_snr_validity(series: pd.Series) -> dict:
    """Calculates the ratio of valid SNRs, UUIDs, and other invalid values.

    A valid snr is 7 characters long.
    A UUID is 32 characters long.

    Args:
        series: The pandas Series containing 'snr' values.

    Returns:
        A dictionary with the ratios for valid, uuid, and other invalid values.
    """
    total_count = len(series)
    valid_snr_count = (series.str.len() == 7).sum()
    uuid_count = (series.str.len() == 32).sum()
    other_invalid_count = total_count - valid_snr_count - uuid_count

    return {
        "valid_snr_ratio": valid_snr_count / total_count if total_count > 0 else 0,
        "uuid_ratio": uuid_count / total_count if total_count > 0 else 0,
        "other_invalid_ratio": other_invalid_count / total_count if total_count > 0 else 0,
    }

def verify_klass_codes(series: pd.Series, valid_codes: Set[str]) -> dict:
    """Verifies categorical codes against a set of valid codes from KLASS.

    Args:
        series: The pandas Series with categorical codes.
        valid_codes: A set of valid codes.

    Returns:
        A dictionary with the ratio of valid codes and a list of invalid ones.
    """
    total_count = len(series)
    valid_mask = series.isin(valid_codes)
    valid_count = valid_mask.sum()
    invalid_codes = list(series[~valid_mask].unique())

    return {
        "valid_code_ratio": valid_count / total_count if total_count > 0 else 0,
        "invalid_codes": invalid_codes,
    }

def calculate_unique_value_count(series: pd.Series) -> int:
    """Calculates the number of unique values in a pandas Series.

    Args:
        series: The pandas Series to calculate the unique value count for.

    Returns:
        The number of unique values as an integer.
    """
    return series.nunique()
