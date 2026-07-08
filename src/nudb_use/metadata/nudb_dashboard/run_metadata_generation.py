import pandas as pd
from typing import Union, Dict, Any, Optional

from nudb_config import settings
from . import file_metadata as fm
from . import variable_quality as vq

def generate_metadata(
    data: Union[str, pd.DataFrame],
    file_path: Optional[str] = None,
    repo_path: str = ".",
    full_klass_verification: bool = False,
) -> Dict[str, Any]:
    """
    Generates a complete metadata dictionary for a given dataset.

    Args:
        data: A file path to a Parquet file or a pandas DataFrame.
        file_path: Optional. The original file path for the data.
        repo_path: Optional. The path to the git repository.
        full_klass_verification: Optional. If True, run the slow, detailed, version-aware KLASS verification.

    Returns:
        A dictionary containing the complete metadata.
    """
    if isinstance(data, str):
        file_path = data if file_path is None else file_path
        df = pd.read_parquet(data)
    else:
        df = data

    # Load missing values mapping from nudb_config
    missing_values_map = settings.constants.get("missing_vals", {})

    # Generate file-level metadata
    metadata = {}
    if file_path:
        metadata = fm.generate_file_metadata(file_path=file_path, repo_path=repo_path)
    
    # Calculate source contribution if column exists
    if "nudb_dataset_id" in df.columns:
        metadata["source_contribution"] = fm.calculate_source_contribution(df["nudb_dataset_id"])

    metadata["column_level_metrics"] = {}

    # Generate column-level metrics
    for col in df.columns:
        series = df[col]
        metrics = {}

        # General metrics for all columns
        missing_values_config = missing_values_map.get(col)
        if missing_values_config is None:
            missing_values = []
        elif isinstance(missing_values_config, list):
            missing_values = missing_values_config
        else:
            missing_values = [missing_values_config]
            
        metrics["fill_rate"] = vq.calculate_fill_rate(series)
        metrics["value_ratios"] = vq.calculate_missing_ratios(series, missing_values)

        # Special metrics for specific columns
        if col == "snr":
            metrics["snr_validity"] = vq.calculate_snr_validity(series)
        
        # Automatic KLASS verification
        if full_klass_verification:
            klass_verification = vq.verify_klass_codes_by_version(series, col)
            if klass_verification is not None:
                metrics["klass_verification_by_version"] = klass_verification
        else:
            klass_verification = vq.verify_klass_codes(series, col)
            if klass_verification is not None:
                metrics["klass_verification"] = klass_verification
        
        metadata["column_level_metrics"][col] = metrics

    return metadata
