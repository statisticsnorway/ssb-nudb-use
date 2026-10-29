from types import SimpleNamespace

import pandas as pd
import pytest

import nudb_use.quality.outdated_variables as outdated_variables
from nudb_use.quality.outdated_variables import check_outdated_variables
from nudb_use.quality.outdated_variables import find_outdated_variables_in_df


def _make_variable(unit: str, comment: str) -> dict[str, str]:
    return {"unit": unit, "outdated_comment": comment}


def test_check_outdated_variables(monkeypatch: pytest.MonkeyPatch) -> None:
    outdated_col = "old_flag"
    data = pd.DataFrame({outdated_col: [1]})

    monkeypatch.setattr(
        outdated_variables,
        "settings_use",
        type(
            "DummySettings",
            (),
            {
                "variables": {
                    outdated_col: _make_variable("utdatert", "drop me"),
                }
            },
        )(),
    )

    errors = check_outdated_variables(data)
    assert len(errors) == 1
    assert outdated_col in str(errors[0])


def test_find_outdated_variables_in_df(monkeypatch: pytest.MonkeyPatch) -> None:
    outdated_col = "OLD_FLAG"
    data = pd.DataFrame({outdated_col: [1]})

    dummy_vars = {
        "old_flag": _make_variable("utdatert", "remove"),
        "keep_me": _make_variable("valid", "keep"),
    }
    monkeypatch.setattr(
        outdated_variables,
        "settings_use",
        type("DummySettings", (), {"variables": dummy_vars})(),
    )

    found = find_outdated_variables_in_df(data)
    assert list(found.keys()) == ["old_flag"]
    assert found["old_flag"]["outdated_comment"] == "remove"


@pytest.mark.parametrize("dataset", [None, "ongoing"])
def test_outdated_includes_rename_mappings(
    monkeypatch: pytest.MonkeyPatch, dataset: str | None
) -> None:
    variables = {
        "current": {"unit": "valid", "renamed_from": ["old"]},
        "other": {"unit": "valid"},
        "retired": _make_variable("utdatert", "remove"),
    }
    monkeypatch.setattr(
        outdated_variables,
        "settings_use",
        SimpleNamespace(
            variables=variables,
            datasets={
                "ongoing": SimpleNamespace(
                    dataset_specific_renames={"ftype": "current"}
                ),
                "completed": SimpleNamespace(
                    dataset_specific_renames={"ftype": "other"}
                ),
            },
        ),
    )
    df = pd.DataFrame(columns=["RETIRED", "OLD", "FTYPE", "current", "unknown"])
    found = find_outdated_variables_in_df(df, dataset_name=dataset)
    assert set(found) == {"retired", "old", "ftype"}
    assert found["old"]["renamed_to"] == {"current": ["variables.renamed_from"]}
    assert set(found["ftype"]["renamed_to"]) == (
        {"current", "other"} if dataset is None else {"current"}
    )
    errors = check_outdated_variables(df, dataset_name=dataset)
    assert len(errors) == 3
    assert "remove" in str(errors[0])
    assert all(text in str(errors[2]) for text in ["ftype", "current", "ongoing"])
    if dataset is None:
        assert "other" in str(errors[2]) and "completed" in str(errors[2])
    assert variables["current"] == {"unit": "valid", "renamed_from": ["old"]}
