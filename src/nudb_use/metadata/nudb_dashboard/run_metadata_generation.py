import pandas as pd
import json
import os
from typing import Union, Dict, Any, Optional

from nudb_config import settings
from . import file_metadata as fm
from . import variable_quality as vq
from fagfunksjoner.paths.versions import next_version_path

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

def write_with_metadata(data: pd.DataFrame, base_path: str, **kwargs):
    """
    Writes a DataFrame to a versioned Parquet file and saves its metadata to a central directory.

    Args:
        data: The DataFrame to save.
        base_path: The base path for the output file (e.g., /path/to/file_v0.parquet).
        **kwargs: Additional keyword arguments to pass to generate_metadata.
    """
    # Determine the final versioned path for the data file
    write_path = next_version_path(base_path)

    # Write the actual data
    data.to_parquet(write_path)

    # Generate the metadata for the file just written
    metadata = generate_metadata(data=data, file_path=write_path, **kwargs)
    
    # Determine the path for the metadata file
    metadata_dir = "/buckets/produkt/nudb-data/metadata/data-quality"
    json_filename = os.path.basename(write_path).replace(".parquet", ".json")
    metadata_path = os.path.join(metadata_dir, json_filename)

    # Create directory if it doesn't exist and save the metadata
    os.makedirs(metadata_dir, exist_ok=True)
    with open(metadata_path, 'w') as f:
        json.dump(metadata, f, indent=2, default=str) # Use default=str to handle numpy types
    
    print(f"Data written to: {write_path}")
    print(f"Metadata written to: {metadata_path}")
