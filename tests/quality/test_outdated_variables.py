from typing import Any

import pandas as pd
import pytest

import nudb_use.quality.outdated_variables as outdated_variables
from nudb_use.quality.outdated_variables import check_outdated_variables
from nudb_use.quality.outdated_variables import find_outdated_variables_in_df


def _make_variable(unit: str, comment: str) -> Any:
    """Create a minimal Variable instance compatible with typeguard."""
    outdated_mod: Any = outdated_variables
    var_cls: Any = outdated_mod.Variable
    if hasattr(var_cls, "model_construct"):
        return var_cls.model_construct(unit=unit, outdated_comment=comment)
    try:
        return var_cls(unit=unit, outdated_comment=comment)
    except Exception:
        var = var_cls.__new__(var_cls)
        var.unit = unit
        var.outdated_comment = comment
        return var


def test_check_outdated_variables(
    avslutta: pd.DataFrame, monkeypatch: pytest.MonkeyPatch
) -> None:
    outdated_col = "old_flag"
    avslutta[outdated_col] = 1

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

    errors = check_outdated_variables(avslutta)
    assert len(errors) == 1
    assert outdated_col in str(errors[0])


def test_find_outdated_variables_in_df(
    avslutta: pd.DataFrame, monkeypatch: pytest.MonkeyPatch
) -> None:
    outdated_col = "OLD_FLAG"
    avslutta[outdated_col] = 1

    dummy_vars = {
        "old_flag": _make_variable("utdatert", "remove"),
        "keep_me": _make_variable("valid", "keep"),
    }
    monkeypatch.setattr(
        outdated_variables,
        "settings_use",
        type("DummySettings", (), {"variables": dummy_vars})(),
    )

    found = find_outdated_variables_in_df(avslutta)
    assert list(found.keys()) == ["old_flag"]
    assert found["old_flag"].outdated_comment == "remove"
