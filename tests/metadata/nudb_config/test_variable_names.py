import importlib
from types import SimpleNamespace
from typing import Any

import pandas as pd
import pytest

variable_names_module = importlib.import_module(
    "nudb_use.metadata.nudb_config.variable_names"
)
from nudb_use.metadata.nudb_config.variable_names import get_cols2drop
from nudb_use.metadata.nudb_config.variable_names import get_cols2keep
from nudb_use.metadata.nudb_config.variable_names import get_cols_in_config
from nudb_use.metadata.nudb_config.variable_names import handle_dataset_specific_renames
from nudb_use.metadata.nudb_config.variable_names import sort_cols_after_config_order
from nudb_use.metadata.nudb_config.variable_names import update_colnames


class FakeSettings(dict[str, Any]):
    def __init__(self, **kwargs: Any) -> None:
        """Minimal settings stand-in for monkeypatching module state."""
        super().__init__(kwargs)
        self.variables: dict[str, Any] = kwargs.get("variables", {})
        self.datasets: dict[str, Any] = kwargs.get("datasets", {})
        self.variables_sort_unit: list[str] | None = kwargs.get("variables_sort_unit")


def test_sort_cols_after_config_order(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_settings = FakeSettings(variables={"a": {}, "b": {}})
    monkeypatch.setattr(variable_names_module, "settings_use", fake_settings)

    df = pd.DataFrame(columns=["C", "B", "A"])

    result = sort_cols_after_config_order(df)

    assert list(result.columns) == ["a", "b", "c"]


def test_get_cols_in_config_and_keep_drop(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_settings = FakeSettings(
        variables={"a": {}, "b": {}},
        datasets={"ds": {"variables": ["a"]}},
    )
    monkeypatch.setattr(variable_names_module, "settings_use", fake_settings)

    assert get_cols_in_config(None) == ["a", "b"]
    with pytest.raises(KeyError):
        get_cols_in_config("missing")

    df = pd.DataFrame(columns=["a", "extra"])
    assert list(get_cols2keep(df)) == ["a"]
    assert list(get_cols2drop(df)) == ["extra"]


def test_update_colnames_uses_metadata(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        variable_names_module,
        "get_var_metadata",
        lambda: pd.DataFrame({"renamed_from": [["old_a"]]}, index=["new_a"]),
    )

    df = pd.DataFrame({"old_a": [1], "stay": [2]})

    result = update_colnames(df)

    assert list(result.columns) == ["new_a", "stay"]


def test_handle_dataset_specific_renames(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_settings = FakeSettings(
        datasets={"ds": SimpleNamespace(dataset_specific_renames={"old": "new"})}
    )
    monkeypatch.setattr(variable_names_module, "settings_use", fake_settings)

    df = pd.DataFrame({"old": [1]})

    result = handle_dataset_specific_renames(df, "ds")

    assert list(result.columns) == ["new"]
