import pandas as pd
import numpy as np
from klass import get_classification
from nudb_config import settings
from typing import Set, List, Optional

def calculate_fill_rate(series: pd.Series) -> float:
    """Calculates the fill rate of a pandas Series."""
    return 1 - (series.isnull().sum() / len(series))

def calculate_missing_ratios(series: pd.Series, missing_values: list) -> dict:
    """Calculates the ratio of missing/nan/filled values in a pandas Series."""
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
    """Calculates the ratio of valid SNRs, UUIDs, and other invalid values."""
    total_count = len(series)
    valid_snr_count = (series.str.len() == 7).sum()
    uuid_count = (series.str.len() == 32).sum()
    other_invalid_count = total_count - valid_snr_count - uuid_count

    return {
        "valid_snr_ratio": valid_snr_count / total_count if total_count > 0 else 0,
        "uuid_ratio": uuid_count / total_count if total_count > 0 else 0,
        "other_invalid_ratio": other_invalid_count / total_count if total_count > 0 else 0,
    }

def verify_klass_codes(series: pd.Series, variable_name: str) -> Optional[dict]:
    """Verifies categorical codes against a set of valid codes from KLASS."""
    try:
        variable_config = settings.variables.get(variable_name)

        if not variable_config or not getattr(variable_config, 'klass_codelist', 0):
            return None

        klass_id = variable_config.klass_codelist
        if not klass_id:
            return None

        classification = get_classification(str(klass_id))
        valid_codes_df = classification.get_codes().data
        valid_codes = set(valid_codes_df['code'])
        
        total_count = len(series)
        valid_mask = series.isin(valid_codes)
        valid_count = valid_mask.sum()
        invalid_codes = list(series[~valid_mask].unique())

        return {
            "valid_code_ratio": valid_count / total_count if total_count > 0 else 0,
            "invalid_codes": invalid_codes,
        }
    except Exception as e:
        print(f"Could not verify KLASS codes for {variable_name}: {e}")
        return None

def calculate_unique_value_count(series: pd.Series) -> int:
    """Calculates the number of unique values in a pandas Series."""
    return series.nunique()
