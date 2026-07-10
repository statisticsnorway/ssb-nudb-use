import pandas as pd
import os
from typing import Union, Optional, list

from nudb_config import settings
from . import file_metadata as fm
from . import variable_quality as vq
from fagfunksjoner.paths.versions import next_version_path
from pydantic import BaseModel

def generate_metadata(
    data: Union[str, pd.DataFrame],
    file_path: Optional[str] = None,
    repo_path: str = ".",
    full_klass_verification: bool = False,
) -> list[BaseModel]:
    """
    Generates a complete metadata dictionary for a given dataset.

    Args:
        data: A file path to a Parquet file or a pandas DataFrame.
        file_path: Optional. The original file path for the data.
        repo_path: Optional. The path to the git repository.
        full_klass_verification: Optional. If True, run the slow, detailed, version-aware KLASS verification.

    Returns:
        A list of Pydantic models containing the complete metadata.
    """
    if isinstance(data, str):
        file_path = data if file_path is None else file_path
        df = pd.read_parquet(data)
    else:
        df = data

    statlog = []

    # Generate linage and release information
    if file_path:
        data_sources = []
        if "nudb_dataset_id" in df.columns:
            data_sources = df["nudb_dataset_id"].unique().tolist()
        
        statlog.append(fm.generate_linage(data_source=data_sources, data_target=[file_path], step="write_with_metadata"))
        statlog.append(fm.generate_release(repo_path=repo_path, data_source=[file_path]))

    # Generate column-level metrics
    if file_path:
        project_name = fm.generate_release(repo_path=repo_path, data_source=[file_path]).statistics_name
        year = fm.get_file_details(file_path)['year']
        quality_results = vq.run_quality_checks(df, project_name, [file_path], str(year) , full_klass_verification)
        statlog.extend(quality_results)

    return statlog

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
    statlog = generate_metadata(data=data, file_path=write_path, **kwargs)
    
    # Determine the path for the metadata file
    metadata_dir = "/buckets/produkt/nudb-data/metadata/data-quality"
    json_filename = os.path.basename(write_path).replace(".parquet", ".jsonl")
    metadata_path = os.path.join(metadata_dir, json_filename)

    # Create directory if it doesn't exist and save the metadata
    os.makedirs(metadata_dir, exist_ok=True)
    with open(metadata_path, 'w') as f:
        for item in statlog:
            f.write(item.model_dump_json() + "\n")
    
    print(f"Data written to: {write_path}")
    print(f"Metadata written to: {metadata_path}")
