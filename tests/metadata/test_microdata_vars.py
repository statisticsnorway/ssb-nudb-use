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
