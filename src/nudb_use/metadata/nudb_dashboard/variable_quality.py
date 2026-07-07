import pandas as pd
import numpy as np

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
        "missing_ratio": missing_count / total_count,
        "nan_ratio": nan_count / total_count,
        "filled_ratio": filled_count / total_count,
    }

def calculate_snr_ratio(series: pd.Series) -> dict:
    """Calculates the ratio of UUIDs to valid values for the 'snr' column.

    A valid snr is 7 characters long. Anything else is considered a UUID.

    Args:
        series: The pandas Series containing 'snr' values.

    Returns:
        A dictionary with the counts for valid, uuid, and the ratio.
    """
    
    valid_count = series.str.len() == 7
    valid_count = valid_count.sum()
    total_count = len(series)
    uuid_count = total_count - valid_count
    
    return {
        "valid_count": valid_count,
        "uuid_count": uuid_count,
        "valid_ratio": valid_count / total_count if total_count > 0 else 0,
    }

def calculate_unique_value_count(series: pd.Series) -> int:
    """Calculates the number of unique values in a pandas Series.

    Args:
        series: The pandas Series to calculate the unique value count for.

    Returns:
        The number of unique values as an integer.
    """
    return series.nunique()