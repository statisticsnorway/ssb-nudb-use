import importlib
from types import SimpleNamespace

import pandas as pd
import pytest

get_variable_info_module = importlib.import_module(
    "nudb_use.metadata.nudb_config.get_variable_info"
)
from nudb_use.metadata.nudb_config.get_variable_info import get_toml_field
from nudb_use.metadata.nudb_config.get_variable_info import get_var_metadata


def test_get_toml_field_returns_value_and_none() -> None:
    assert get_toml_field({"foo": 1}, "foo") == 1
    assert get_toml_field({"foo": 1}, "bar") is None


def test_get_var_metadata(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_settings = SimpleNamespace(
        variables={
            "a": {"dtype": "STRING", "desc": "A"},
            "b": {"dtype": "INTEGER", "desc": "B"},
        }
    )
    monkeypatch.setattr(get_variable_info_module, "settings_use", fake_settings)

    df = get_var_metadata(["b"])

    expected = pd.DataFrame(
        [{"variable": "b", "dtype": "INTEGER", "desc": "B"}]
    ).set_index("variable")

    pd.testing.assert_frame_equal(df, expected)
