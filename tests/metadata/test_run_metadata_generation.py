import pandas as pd
import os
import json
import pytest
from ssb_nudb_use.metadata.nudb_dashboard.run_metadata_generation import write_with_metadata

@pytest.fixture
def dummy_dataframe() -> pd.DataFrame:
    return pd.DataFrame({
        "navn": ["per", "paal", "askeladden"],
        "alder": [1, 2, 3],
        "nudb_dataset_id": ["v1", "v1", "v2"],
        "snr": ["1234567", "2345678", "3456789"],
    })

def test_write_with_metadata(dummy_dataframe, tmp_path):
    base_path = os.path.join(tmp_path, "test_data_v0.parquet")
    
    # Create a dummy repo for testing
    repo_path = os.path.join(tmp_path, "dummy_repo")
    os.makedirs(repo_path)
    # create a dummy pyproject.toml
    with open(os.path.join(repo_path, "pyproject.toml"), "w") as f:
        f.write("[project]\nname = \"dummy-project\"\n")

    write_with_metadata(dummy_dataframe, base_path, repo_path=repo_path)

    # Check that the output files were created
    data_path = base_path.replace("_v0", "_v1")
    metadata_path = data_path.replace(".parquet", ".jsonl")
    assert os.path.exists(data_path)
    assert os.path.exists(metadata_path)

    # Check the contents of the metadata file
    with open(metadata_path, "r") as f:
        lines = f.readlines()
    
    # Expecting 1 linage, 1 release, and 4 * 3 = 12 quality control results
    assert len(lines) == 1 + 1 + (4 * 3) 

    # Check the contents of the first line (linage)
    linage = json.loads(lines[0])
    assert linage["schema_version"] == "1.0.0"
    assert linage["data_source"] == ["v1", "v2"]
    assert linage["data_target"] == [data_path]
    assert linage["step"] == "write_with_metadata"

    # Check the contents of the second line (release)
    release = json.loads(lines[1])
    assert release["schema_version"] == "1.0.0"
    assert release["statistics_name"] == "dummy-project"
    assert release["data_source"] == [data_path]

    # Check one of the quality control results
    quality_result = json.loads(lines[2])
    assert quality_result["schema_version"] == "2.0.0"
    assert quality_result["statistics_name"] == "dummy-project"
    assert quality_result["quality_control_id"] == "fill_rate"
