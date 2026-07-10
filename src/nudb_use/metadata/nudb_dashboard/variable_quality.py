import pandas as pd
import numpy as np
from klass import get_classification
from nudb_config import settings
from typing import Set, List, Optional, Dict
from collections import defaultdict
from ssb_poc_statlog_model.quality_control_description import QualityControlDescription, QualityControlType, Variable
from ssb_poc_statlog_model.quality_control_result import QualityControlResult, QualityControlResults
from datetime import datetime

QUALITY_CONTROL_DESCRIPTIONS: Dict[str, QualityControlDescription] = {
    "fill_rate": QualityControlDescription(
        quality_control_id="fill_rate",
        quality_control_description="Calculates the fill rate of a column.",
        quality_control_type=QualityControlType.I,
        variables=[Variable(variable_description="*")]
    ),
    "missing_ratios": QualityControlDescription(
        quality_control_id="missing_ratios",
        quality_control_description="Calculates the ratio of missing/nan/filled values.",
        quality_control_type=QualityControlType.I,
        variables=[Variable(variable_description="*")]
    ),
    "snr_validity": QualityControlDescription(
        quality_control_id="snr_validity",
        quality_control_description="Calculates the ratio of valid SNRs, UUIDs, and other invalid values.",
        quality_control_type=QualityControlType.S,
        variables=[Variable(variable_description="snr")]
    ),
    "klass_code_verification": QualityControlDescription(
        quality_control_id="klass_code_verification",
        quality_control_description="Verifies categorical codes against the latest version of a KLASS classification.",
        quality_control_type=QualityControlType.S,
        variables=[Variable(variable_description="*")]
    ),
    "klass_code_verification_by_version": QualityControlDescription(
        quality_control_id="klass_code_verification_by_version",
        quality_control_description="Verifies codes against all historical versions of a KLASS classification.",
        quality_control_type=QualityControlType.S,
        variables=[Variable(variable_description="*")]
    ),
    "unique_value_count": QualityControlDescription(
        quality_control_id="unique_value_count",
        quality_control_description="Calculates the number of unique values in a column.",
        quality_control_type=QualityControlType.I,
        variables=[Variable(variable_description="*")]
    ),
}

def calculate_fill_rate(series: pd.Series, quality_control_id: str, statistics_name: str, data_location: list[str], data_period: str) -> QualityControlResult:
    """Calculates the fill rate of a pandas Series."""
    fill_rate = 1 - (series.isnull().sum() / len(series))
    return QualityControlResult(
        quality_control_id=quality_control_id,
        statistics_name=statistics_name,
        data_location=data_location,
        data_period=data_period,
        quality_control_datetime=datetime.now().isoformat(),
        quality_control_results=QualityControlResults.field_0 if fill_rate == 1.0 else QualityControlResults.field_1,
        quality_result_comment=f"Fill rate: {fill_rate:.2f}"
    )

def calculate_missing_ratios(series: pd.Series, missing_values: list, quality_control_id: str, statistics_name: str, data_location: list[str], data_period: str) -> QualityControlResult:
    """Calculates the ratio of missing/nan/filled values in a pandas Series."""
    total_count = len(series)
    nan_count = series.isnull().sum()
    missing_count = series.isin(missing_values).sum()
    filled_count = total_count - nan_count - missing_count
    
    ratios = {
        "missing_ratio": missing_count / total_count if total_count > 0 else 0,
        "nan_ratio": nan_count / total_count if total_count > 0 else 0,
        "filled_ratio": filled_count / total_count if total_count > 0 else 0,
    }

    return QualityControlResult(
        quality_control_id=quality_control_id,
        statistics_name=statistics_name,
        data_location=data_location,
        data_period=data_period,
        quality_control_datetime=datetime.now().isoformat(),
        quality_control_results=QualityControlResults.field_0 if ratios["missing_ratio"] == 0 and ratios["nan_ratio"] == 0 else QualityControlResults.field_1,
        quality_result_comment=str(ratios)
    )

def calculate_snr_validity(series: pd.Series, quality_control_id: str, statistics_name: str, data_location: list[str], data_period: str) -> QualityControlResult:
    """Calculates the ratio of valid SNRs, UUIDs, and other invalid values."""
    total_count = len(series)
    valid_snr_count = (series.str.len() == 7).sum()
    uuid_count = (series.str.len() == 32).sum()
    other_invalid_count = total_count - valid_snr_count - uuid_count

    ratios = {
        "valid_snr_ratio": valid_snr_count / total_count if total_count > 0 else 0,
        "uuid_ratio": uuid_count / total_count if total_count > 0 else 0,
        "other_invalid_ratio": other_invalid_count / total_count if total_count > 0 else 0,
    }

    return QualityControlResult(
        quality_control_id=quality_control_id,
        statistics_name=statistics_name,
        data_location=data_location,
        data_period=data_period,
        quality_control_datetime=datetime.now().isoformat(),
        quality_control_results=QualityControlResults.field_0 if ratios["other_invalid_ratio"] == 0 else QualityControlResults.field_1,
        quality_result_comment=str(ratios)
    )

def verify_klass_codes(series: pd.Series, variable_name: str, quality_control_id: str, statistics_name: str, data_location: list[str], data_period: str) -> Optional[QualityControlResult]:
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

        result = {
            "valid_code_ratio": valid_count / total_count if total_count > 0 else 0,
            "invalid_codes": invalid_codes[:20],
        }

        return QualityControlResult(
            quality_control_id=quality_control_id,
            statistics_name=statistics_name,
            data_location=data_location,
            data_period=data_period,
            quality_control_datetime=datetime.now().isoformat(),
            quality_control_results=QualityControlResults.field_0 if result["valid_code_ratio"] == 1.0 else QualityControlResults.field_1,
            quality_result_comment=str(result)
        )
    except Exception as e:
        return QualityControlResult(
            quality_control_id=quality_control_id,
            statistics_name=statistics_name,
            data_location=data_location,
            data_period=data_period,
            quality_control_datetime=datetime.now().isoformat(),
            quality_control_results=QualityControlResults.field_1,
            quality_control_run_exception=f"Could not verify KLASS codes for {variable_name}: {e}"
        )

def verify_klass_codes_by_version(series: pd.Series, variable_name: str, quality_control_id: str, statistics_name: str, data_location: list[str], data_period: str) -> Optional[QualityControlResult]:
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
        
        return QualityControlResult(
            quality_control_id=quality_control_id,
            statistics_name=statistics_name,
            data_location=data_location,
            data_period=data_period,
            quality_control_datetime=datetime.now().isoformat(),
            quality_control_results=QualityControlResults.field_0 if results["invalid"]["count"] == 0 else QualityControlResults.field_1,
            quality_result_comment=str(results)
        )

    except Exception as e:
        return QualityControlResult(
            quality_control_id=quality_control_id,
            statistics_name=statistics_name,
            data_location=data_location,
            data_period=data_period,
            quality_control_datetime=datetime.now().isoformat(),
            quality_control_results=QualityControlResults.field_1,
            quality_control_run_exception=f"Could not perform version-aware KLASS verification for {variable_name}: {e}"
        )

def calculate_unique_value_count(series: pd.Series, quality_control_id: str, statistics_name: str, data_location: list[str], data_period: str) -> QualityControlResult:
    """Calculates the number of unique values in a pandas Series."""
    unique_count = series.nunique()
    return QualityControlResult(
        quality_control_id=quality_control_id,
        statistics_name=statistics_name,
        data_location=data_location,
        data_period=data_period,
        quality_control_datetime=datetime.now().isoformat(),
        quality_control_results=QualityControlResults.field_0,
        quality_result_comment=f"Unique value count: {unique_count}"
    )

def run_quality_checks(df: pd.DataFrame, statistics_name: str, data_location: list[str], data_period: str, full_klass_verification: bool = False) -> list[QualityControlResult]:
    """Runs all quality checks on a DataFrame."""
    results = []
    for col in df.columns:
        results.append(calculate_fill_rate(df[col], "fill_rate", statistics_name, data_location, data_period))
        results.append(calculate_missing_ratios(df[col], [], "missing_ratios", statistics_name, data_location, data_period))
        results.append(calculate_unique_value_count(df[col], "unique_value_count", statistics_name, data_location, data_period))

        if col == "snr":
            results.append(calculate_snr_validity(df[col], "snr_validity", statistics_name, data_location, data_period))

        if full_klass_verification:
            klass_result = verify_klass_codes_by_version(df[col], col, "klass_code_verification_by_version", statistics_name, data_location, data_period)
        else:
            klass_result = verify_klass_codes(df[col], col, "klass_code_verification", statistics_name, data_location, data_period)
        
        if klass_result:
            results.append(klass_result)

    return results
