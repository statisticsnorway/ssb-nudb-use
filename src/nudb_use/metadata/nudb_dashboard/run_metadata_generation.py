import pandas as pd
from typing import Union, Dict, Set, Any

from .file_metadata import generate_file_metadata
from . import variable_quality as vq

def generate_metadata(
    data: Union[str, pd.DataFrame],
    file_path: str,
    repo_path: str,
    klass_definitions: Dict[str, Set[str]],
    missing_values_map: Dict[str, list],
) -> Dict[str, Any]:
    """
    Generates a complete metadata dictionary for a given dataset.

    Args:
        data: A file path to a Parquet file or a pandas DataFrame.
        file_path: The original file path for the data (used for metadata).
        repo_path: The path to the git repository for environment details.
        klass_definitions: A dict where keys are column names and values are sets of valid KLASS codes.
        missing_values_map: A dict mapping column names to lists of missing value representations.

    Returns:
        A dictionary containing the complete metadata.
    """
    if isinstance(data, str):
        df = pd.read_parquet(data)
    else:
        df = data

    # Generate file-level metadata
    metadata = generate_file_metadata(file_path=file_path, repo_path=repo_path)
    metadata["column_level_metrics"] = {}

    # Generate column-level metrics
    for col in df.columns:
        series = df[col]
        metrics = {}

        # General metrics for all columns
        metrics["fill_rate"] = vq.calculate_fill_rate(series)
        missing_values = missing_values_map.get(col, [])
        metrics["value_ratios"] = vq.calculate_missing_ratios(series, missing_values)

        # Special metrics for specific columns
        if col == "snr":
            metrics["snr_validity"] = vq.calculate_snr_validity(series)
        
        if col in klass_definitions:
            valid_codes = klass_definitions[col]
            metrics["klass_verification"] = vq.verify_klass_codes(series, valid_codes)
        
        metadata["column_level_metrics"][col] = metrics

    return metadata
