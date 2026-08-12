import pandas as pd
import numpy as np
import json
from klass import get_classification
from nudb_config import settings
from typing import Set, List, Optional, Dict
from collections import defaultdict
from ssb_poc_statlog_model.quality_control_description import QualityControlDescription, QualityControlType, Variable
from ssb_poc_statlog_model.quality_control_result import QualityControlResult, QualityControlResults
from datetime import datetime, timezone

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
    "boolean_ratio": QualityControlDescription(
        quality_control_id="boolean_ratio",
        quality_control_description="Calculates the ratio of True and False values for a boolean column.",
        quality_control_type=QualityControlType.S,
        variables=[Variable(variable_description="snr_mrk")]
    ),
    "min_max_value": QualityControlDescription(
        quality_control_id="min_max_value",
        quality_control_description="Calculates the min and max values of a column.",
        quality_control_type=QualityControlType.I,
        variables=[Variable(variable_description="utd_skoleaar_start")]
    ),
}

def calculate_fill_rate(series: pd.Series, quality_control_id: str, statistics_name: str, data_location: list[str], data_period: str) -> QualityControlResult:
    """Calculates the fill rate of a pandas Series."""
    total_count = len(series)
    non_null_count = total_count - series.isnull().sum()
    fill_rate = non_null_count / total_count if total_count > 0 else 0
    
    result_data = {
        "fill_rate": float(fill_rate),
        "filled_count": int(non_null_count),
        "total_count": int(total_count)
    }

    return QualityControlResult(
        quality_control_id=quality_control_id,
        statistics_name=statistics_name,
        data_location=data_location,
        data_period=data_period,
        quality_control_datetime=datetime.now(timezone.utc).isoformat(),
        quality_control_results=QualityControlResults.field_0 if fill_rate == 1.0 else QualityControlResults.field_1,
        quality_result_comment=str(result_data)
    )

def calculate_missing_ratios(series: pd.Series, missing_values: list, quality_control_id: str, statistics_name: str, data_location: list[str], data_period: str) -> QualityControlResult:
    """Calculates the ratio of missing/nan/filled values in a pandas Series."""
    total_count = len(series)
    nan_count = series.isnull().sum()
    missing_count = series.isin(missing_values).sum()
    filled_count = total_count - nan_count - missing_count
    
    result_data = {
        "missing_ratio": float(missing_count / total_count if total_count > 0 else 0),
        "nan_ratio": float(nan_count / total_count if total_count > 0 else 0),
        "filled_ratio": float(filled_count / total_count if total_count > 0 else 0),
        "missing_count": int(missing_count),
        "nan_count": int(nan_count),
        "filled_count": int(filled_count),
        "total_count": int(total_count)
    }

    return QualityControlResult(
        quality_control_id=quality_control_id,
        statistics_name=statistics_name,
        data_location=data_location,
        data_period=data_period,
        quality_control_datetime=datetime.now(timezone.utc).isoformat(),
        quality_control_results=QualityControlResults.field_0 if result_data["missing_ratio"] == 0 and result_data["nan_ratio"] == 0 else QualityControlResults.field_1,
        quality_result_comment=str(result_data)
    )

def calculate_snr_validity(series: pd.Series, quality_control_id: str, statistics_name: str, data_location: list[str], data_period: str) -> QualityControlResult:
    """Calculates the ratio of valid SNRs, UUIDs, and other invalid values."""
    total_count = len(series)
    valid_snr_count = (series.str.len() == 7).sum()
    uuid_count = (series.str.len() == 32).sum()
    other_invalid_count = total_count - valid_snr_count - uuid_count

    ratios = {
        "valid_snr_ratio": float(valid_snr_count / total_count if total_count > 0 else 0),
        "uuid_ratio": float(uuid_count / total_count if total_count > 0 else 0),
        "other_invalid_ratio": float(other_invalid_count / total_count if total_count > 0 else 0),
    }

    return QualityControlResult(
        quality_control_id=quality_control_id,
        statistics_name=statistics_name,
        data_location=data_location,
        data_period=data_period,
        quality_control_datetime=datetime.now(timezone.utc).isoformat(),
        quality_control_results=QualityControlResults.field_0 if ratios["other_invalid_ratio"] == 0 else QualityControlResults.field_1,
        quality_result_comment=str(ratios)
    )

def verify_klass_codes(df: pd.DataFrame, variable_name: str, quality_control_id: str, statistics_name: str, data_location: list[str], data_period: str) -> Optional[QualityControlResult]:
    """Verifies categorical codes against the latest version of a KLASS classification."""
    try:
        if 'utd_skoleaar_start' in df.columns:
            latest_year = df['utd_skoleaar_start'].max()
            df_filtered = df[df['utd_skoleaar_start'] == latest_year]
            series = df_filtered[variable_name]
        else:
            series = df[variable_name]

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
            "valid_code_ratio": float(valid_count / total_count if total_count > 0 else 0),
            "invalid_codes": invalid_codes[:20],
            "valid_count": int(valid_count),
            "total_count": int(total_count),
        }

        return QualityControlResult(
            quality_control_id=quality_control_id,
            statistics_name=statistics_name,
            data_location=data_location,
            data_period=data_period,
            quality_control_datetime=datetime.now(timezone.utc).isoformat(),
            quality_control_results=QualityControlResults.field_0 if result["valid_code_ratio"] == 1.0 else QualityControlResults.field_1,
            quality_result_comment=str(result)
        )
    except Exception as e:
        return QualityControlResult(
            quality_control_id=quality_control_id,
            statistics_name=statistics_name,
            data_location=data_location,
            data_period=data_period,
            quality_control_datetime=datetime.now(timezone.utc).isoformat(),
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
            data["ratio"] = float(data["count"] / total_count if total_count > 0 else 0)
        results["invalid"]["ratio"] = float(results["invalid"]["count"] / total_count if total_count > 0 else 0)
        
        # Convert defaultdict to dict for the final output
        results["validity_by_version"] = dict(results["validity_by_version"])
        
        return QualityControlResult(
            quality_control_id=quality_control_id,
            statistics_name=statistics_name,
            data_location=data_location,
            data_period=data_period,
            quality_control_datetime=datetime.now(timezone.utc).isoformat(),
            quality_control_results=QualityControlResults.field_0 if results["invalid"]["count"] == 0 else QualityControlResults.field_1,
            quality_result_comment=str(results)
        )

    except Exception as e:
        return QualityControlResult(
            quality_control_id=quality_control_id,
            statistics_name=statistics_name,
            data_location=data_location,
            data_period=data_period,
            quality_control_datetime=datetime.now(timezone.utc).isoformat(),
            quality_control_results=QualityControlResults.field_1,
            quality_control_run_exception=f"Could not perform version-aware KLASS verification for {variable_name}: {e}"
        )

def calculate_boolean_ratio(series: pd.Series, quality_control_id: str, statistics_name: str, data_location: list[str], data_period: str) -> Optional[QualityControlResult]:
    """Calculates the ratio of True and False values in a boolean pandas Series."""
    if pd.api.types.is_numeric_dtype(series):
        series = series.astype(bool)

    if not pd.api.types.is_bool_dtype(series):
        return None

    total_count = len(series)
    true_count = series.sum()
    false_count = (series == False).sum()

    result_data = {
        "true_ratio": float(true_count / total_count if total_count > 0 else 0),
        "false_ratio": float(false_count / total_count if total_count > 0 else 0),
        "true_count": int(true_count),
        "false_count": int(false_count),
        "total_count": int(total_count),
    }

    return QualityControlResult(
        quality_control_id=quality_control_id,
        statistics_name=statistics_name,
        data_location=data_location,
        data_period=data_period,
        quality_control_datetime=datetime.now(timezone.utc).isoformat(),
        quality_control_results=QualityControlResults.field_0,
        quality_result_comment=str(result_data)
    )

def calculate_unique_value_count(series: pd.Series, quality_control_id: str, statistics_name: str, data_location: list[str], data_period: str) -> QualityControlResult:
    """Calculates the number of unique values in a pandas Series."""
    unique_count = series.nunique()
    return QualityControlResult(
        quality_control_id=quality_control_id,
        statistics_name=statistics_name,
        data_location=data_location,
        data_period=data_period,
        quality_control_datetime=datetime.now(timezone.utc).isoformat(),
        quality_control_results=QualityControlResults.field_0,
        quality_result_comment=f"Unique value count: {unique_count}"
    )

def calculate_min_max(series: pd.Series, quality_control_id: str, statistics_name: str, data_location: list[str], data_period: str) -> Optional[QualityControlResult]:
    """Calculates the min and max values of a series after converting to numeric."""
    try:
        # Convert to numeric, coercing errors will turn non-numeric values into NaT/NaN
        numeric_series = pd.to_numeric(series, errors='coerce').dropna()
        if numeric_series.empty:
            return None
        
        min_val = numeric_series.min()
        max_val = numeric_series.max()

        result_data = {
            "min": int(min_val),
            "max": int(max_val),
        }
        
        return QualityControlResult(
            quality_control_id=quality_control_id,
            statistics_name=statistics_name,
            data_location=data_location,
            data_period=data_period,
            quality_control_datetime=datetime.now(timezone.utc).isoformat(),
            quality_control_results=QualityControlResults.field_0,
            quality_result_comment=str(result_data)
        )
    except Exception as e:
        return QualityControlResult(
            quality_control_id=quality_control_id,
            statistics_name=statistics_name,
            data_location=data_location,
            data_period=data_period,
            quality_control_datetime=datetime.now(timezone.utc).isoformat(),
            quality_control_results=QualityControlResults.field_1,
            quality_control_run_exception=f"Could not calculate min/max for {series.name}: {e}"
        )

def calculate_unique_snr_per_year(df: pd.DataFrame, quality_control_id: str, statistics_name: str, data_location: list[str], data_period: str) -> Optional[QualityControlResult]:
    """Calculates the number of unique SNRs per utd_skoleaar_start."""
    if 'utd_skoleaar_start' not in df.columns or 'snr' not in df.columns:
        return None

    try:
        snr_per_year = df.groupby('utd_skoleaar_start')['snr'].nunique().to_dict()
        
        # Convert keys to string to be JSON compliant
        snr_per_year = {str(k): v for k, v in snr_per_year.items()}

        return QualityControlResult(
            quality_control_id=quality_control_id,
            statistics_name=statistics_name,
            data_location=data_location,
            data_period=data_period,
            quality_control_datetime=datetime.now(timezone.utc).isoformat(),
            quality_control_results=QualityControlResults.field_0,
            quality_result_comment=json.dumps(snr_per_year)
        )
    except Exception as e:
        return QualityControlResult(
            quality_control_id=quality_control_id,
            statistics_name=statistics_name,
            data_location=data_location,
            data_period=data_period,
            quality_control_datetime=datetime.now(timezone.utc).isoformat(),
            quality_control_results=QualityControlResults.field_1,
            quality_control_run_exception=f"Could not calculate unique snr per year: {e}"
        )

def calculate_nudb_dataset_id_distribution(df: pd.DataFrame, quality_control_id: str, statistics_name: str, data_location: list[str], data_period: str) -> Optional[QualityControlResult]:
    """Calculates the distribution of nudb_dataset_id."""
    if 'nudb_dataset_id' not in df.columns:
        return None

    try:
        # Filter for the highest year in utd_skoleaar_start
        if 'utd_skoleaar_start' in df.columns:
            latest_year = df['utd_skoleaar_start'].max()
            df_filtered = df[df['utd_skoleaar_start'] == latest_year]
            distribution = df_filtered['nudb_dataset_id'].value_counts().to_dict()
        else:
            distribution = df['nudb_dataset_id'].value_counts().to_dict()
        
        # Convert keys to string to be JSON compliant
        distribution = {str(k): v for k, v in distribution.items()}

        return QualityControlResult(
            quality_control_id=quality_control_id,
            statistics_name=statistics_name,
            data_location=data_location,
            data_period=data_period,
            quality_control_datetime=datetime.now(timezone.utc).isoformat(),
            quality_control_results=QualityControlResults.field_0,
            quality_result_comment=json.dumps(distribution)
        )
    except Exception as e:
        return QualityControlResult(
            quality_control_id=quality_control_id,
            statistics_name=statistics_name,
            data_location=data_location,
            data_period=data_period,
            quality_control_datetime=datetime.now(timezone.utc).isoformat(),
            quality_control_results=QualityControlResults.field_1,
            quality_control_run_exception=f"Could not calculate nudb_dataset_id distribution: {e}"
        )


def run_quality_checks(df: pd.DataFrame, statistics_name: str, data_location: list[str], data_period: str, full_klass_verification: bool = False) -> list[QualityControlResult]:
    """Runs all quality checks on a DataFrame."""
    results = []
    for col in df.columns:
        results.append(calculate_fill_rate(df[col], f"fill_rate_{col}", statistics_name, data_location, data_period))
        results.append(calculate_missing_ratios(df[col], [], f"missing_ratios_{col}", statistics_name, data_location, data_period))
        results.append(calculate_unique_value_count(df[col], f"unique_value_count_{col}", statistics_name, data_location, data_period))

        if col == "snr":
            results.append(calculate_snr_validity(df[col], f"snr_validity_{col}", statistics_name, data_location, data_period))

        if col == "snr_mrk":
            print(f"--- DEBUG: Found snr_mrk column, calling calculate_boolean_ratio ---")
            boolean_ratio_result = calculate_boolean_ratio(df[col], f"boolean_ratio_{col}", statistics_name, data_location, data_period)
            if boolean_ratio_result:
                results.append(boolean_ratio_result)

        if col == "utd_skoleaar_start":
            min_max_result = calculate_min_max(df[col], f"min_max_{col}", statistics_name, data_location, data_period)
            if min_max_result:
                results.append(min_max_result)

        if full_klass_verification:
            klass_result = verify_klass_codes_by_version(df[col], col, f"klass_code_verification_by_version_{col}", statistics_name, data_location, data_period)
        else:
            klass_result = verify_klass_codes(df, col, f"klass_code_verification_{col}", statistics_name, data_location, data_period)
        
        if klass_result:
            results.append(klass_result)

    unique_snr_per_year_result = calculate_unique_snr_per_year(df, "unique_snr_per_year", statistics_name, data_location, data_period)
    if unique_snr_per_year_result:
        results.append(unique_snr_per_year_result)

    nudb_dataset_id_dist_result = calculate_nudb_dataset_id_distribution(df, "nudb_dataset_id_distribution", statistics_name, data_location, data_period)
    if nudb_dataset_id_dist_result:
        results.append(nudb_dataset_id_dist_result)

    return results
