import pandas as pd
import numpy as np
from klass import get_classification
from nudb_config import settings
from typing import Set, List, Optional
from collections import defaultdict

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
    """Verifies categorical codes against the latest version of a KLASS classification."""
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

def verify_klass_codes_by_version(series: pd.Series, variable_name: str) -> Optional[dict]:
    """Verifies codes against all historical versions of a KLASS classification."""
    try:
        variable_config = settings.variables.get(variable_name)
        if not variable_config or not getattr(variable_config, 'klass_codelist', 0):
            return None

        klass_id = variable_config.klass_codelist
        if not klass_id:
            return None

        classification = get_classification(str(klass_id))
        
        # Create a map of all codes from all time
        code_to_version_map = {}
        for version_info in classification.versions:
            from_date = version_info['validFrom']
            to_date = version_info.get('validTo', 'present')
            version_key = f"{from_date}_to_{to_date}"
            
            # Get codes that were valid at the start of this version's time period
            version_codes_df = classification.get_codes(from_date=from_date).data
            for code in version_codes_df['code']:
                if code not in code_to_version_map:
                    code_to_version_map[code] = version_key

        # Categorize codes from the input series
        total_count = len(series)
        results = {
            "validity_by_version": defaultdict(lambda: {"count": 0}),
            "invalid": {"count": 0, "values": []}
        }
        unique_codes = series.value_counts()

        for code, count in unique_codes.items():
            if code in code_to_version_map:
                version_key = code_to_version_map[code]
                results["validity_by_version"][version_key]["count"] += count
            else:
                results["invalid"]["count"] += count
                results["invalid"]["values"].append(code)

        # Calculate ratios
        for version_key, data in results["validity_by_version"].items():
            data["ratio"] = data["count"] / total_count if total_count > 0 else 0
        results["invalid"]["ratio"] = results["invalid"]["count"] / total_count if total_count > 0 else 0
        
        # Convert defaultdict to dict for the final output
        results["validity_by_version"] = dict(results["validity_by_version"])
        return dict(results)

    except Exception as e:
        print(f"Could not perform version-aware KLASS verification for {variable_name}: {e}")
        return None

def calculate_unique_value_count(series: pd.Series) -> int:
    """Calculates the number of unique values in a pandas Series."""
    return series.nunique()
