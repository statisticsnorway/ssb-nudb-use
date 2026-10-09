from typing import Any
from unittest.mock import MagicMock

import pandas as pd

from nudb_use.metadata import get_microdata_variables_overview


def test_get_microdata_variables_overview(
    igang: pd.DataFrame,
    avslutta: pd.DataFrame,
    eksamen: pd.DataFrame,
    freg_situttak: pd.DataFrame,
    snrkat: pd.DataFrame,
    slekt: pd.DataFrame,
    tmp_path: Any,
    monkeypatch: Any,
) -> None:
    from tests.datasets.test_nudbdata import patch_nudb_database

    patch_nudb_database(
        igang,
        avslutta,
        eksamen,
        freg_situttak,
        snrkat,
        slekt,
        tmp_path,
        monkeypatch,
    )

    # Mock Vardef.get_variable_definition_by_shortname
    mock_vardef_info = MagicMock()
    mock_vardef_info.model_dump.return_value = {
        "name": {"nb": "Test Variabel Navn"},
        "definition": {"nb": "Dette er en testbeskrivelse."},
    }

    monkeypatch.setattr(
        "dapla_metadata.variable_definitions.Vardef.get_variable_definition_by_shortname",
        lambda short_name: mock_vardef_info,
    )

    overview = get_microdata_variables_overview("utd_hoeyeste_nus2000")

    assert isinstance(overview, pd.DataFrame)
    assert not overview.empty
    assert "Variabel" in overview.columns
    assert "Fullt Navn" in overview.columns
    assert "Min_aar" in overview.columns
    assert "Max_aar" in overview.columns
    assert "Beskrivelse" in overview.columns

    # Check that it fetched the mock Vardef info
    assert (
        overview.loc[overview["Variabel"] == "utd_hoeyeste_nus2000", "Fullt Navn"].iloc[
            0
        ]
        == "Test Variabel Navn"
    )
    assert (
        overview.loc[
            overview["Variabel"] == "utd_hoeyeste_nus2000", "Beskrivelse"
        ].iloc[0]
        == "Dette er en testbeskrivelse."
    )


def test_split_microdata_dataset(tmp_path: Any) -> None:
    from nudb_use import split_microdata_dataset

    # Create dummy DataFrame
    df = pd.DataFrame(
        {
            "snr": ["1", "2", "3", "4"],
            "fnr": ["111", "222", "333", pd.NA],  # 4 has a missing FNR
            "var1": [10.0, 20.0, pd.NA, 40.0],  # 3 has a null var1
            "var2": ["A", pd.NA, "C", "D"],  # 2 has a null var2
            "utd_aktivitet_start": [
                "2020-01-01",
                "2021-01-01",
                "2022-01-01",
                "2023-01-01",
            ],
        }
    )

    # Test splitting with fnr as id_col, expecting 4 to be dropped because fnr is NA
    result = split_microdata_dataset(
        df,
        id_col="fnr",
        auto_detect_start_stop=False,
    )

    # Both var1 and var2 should be present in results
    assert "var1" in result
    assert "var2" in result

    # Check var1 split DataFrame (should only have fnr and var1, dropping any rows where var1 is NA, or fnr is NA)
    # Row 3 had NA var1, Row 4 had NA fnr. So only row 1 and 2 should remain.
    var1_df = result["var1"]
    assert len(var1_df) == 2
    assert set(var1_df.columns) == {"fnr", "var1"}
    assert list(var1_df["fnr"]) == ["111", "222"]
    assert list(var1_df["var1"]) == [10.0, 20.0]

    # Check var2 split DataFrame
    # Row 2 had NA var2, Row 4 had NA fnr. So only row 1 and 3 should remain.
    var2_df = result["var2"]
    assert len(var2_df) == 2
    assert set(var2_df.columns) == {"fnr", "var2"}
    assert list(var2_df["fnr"]) == ["111", "333"]

    # Test automatic detection of start/stop columns
    result_auto = split_microdata_dataset(
        df,
        id_col="fnr",
        auto_detect_start_stop=True,
    )
    # The columns of var1_df should now contain 'utd_aktivitet_start' in the exact order: id, variable, start/stop key
    var1_auto = result_auto["var1"]
    assert list(var1_auto.columns) == ["fnr", "var1", "utd_aktivitet_start"]

    # Test saving to disk as parquet
    split_microdata_dataset(
        df,
        id_col="fnr",
        output_dir=tmp_path,
        file_format="parquet",
    )
    assert (tmp_path / "var1.parquet").exists()
    assert (tmp_path / "var2.parquet").exists()

    # Load back and verify
    loaded_var1 = pd.read_parquet(tmp_path / "var1.parquet")
    assert len(loaded_var1) == 2
