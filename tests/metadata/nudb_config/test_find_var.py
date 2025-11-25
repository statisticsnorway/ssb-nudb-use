import importlib
from types import SimpleNamespace
from typing import Any

import pytest

find_var_module = importlib.import_module("nudb_use.metadata.nudb_config.find_var")
from nudb_use.metadata.nudb_config.find_var import find_var
from nudb_use.metadata.nudb_config.find_var import find_vars
from nudb_use.metadata.nudb_config.find_var import variables_missing_from_config


class FakeSettings:
    def __init__(self, variables: dict[str, Any]):
        """Lightweight settings stand-in for monkeypatching."""
        self.variables = variables


def test_find_var_hits_exact_and_adds_klass(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_settings = FakeSettings(
        variables={
            "new": SimpleNamespace(
                name="new",
                renamed_from=None,
                klass_codelist=123,
                klass_variant=456,
            )
        }
    )
    monkeypatch.setattr(find_var_module, "settings", fake_settings)
    monkeypatch.setattr(
        find_var_module,
        "klass",
        SimpleNamespace(
            KlassClassification=lambda x: f"class-{x}",
            KlassVariant=lambda x: f"variant-{x}",
        ),
    )

    result = find_var("NEW")

    assert result is not None
    assert result["klass_codelist_metadata"] == "class-123"
    assert result["klass_variant_metadata"] == "variant-456"


def test_find_var_resolves_renamed(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_settings = FakeSettings(
        variables={"new": SimpleNamespace(name="new", renamed_from=["old"])}
    )
    monkeypatch.setattr(find_var_module, "settings", fake_settings)
    monkeypatch.setattr(find_var_module, "klass", SimpleNamespace())

    result = find_var("old")

    assert result is not None
    assert result["name"] == "new"


def test_find_var_missing_returns_none(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(find_var_module, "settings", FakeSettings(variables={}))
    monkeypatch.setattr(find_var_module, "klass", SimpleNamespace())

    assert find_var("missing") is None


def test_find_vars_maps_multiple(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_settings = FakeSettings(
        variables={
            "keep": {"name": "keep", "renamed_from": []},
        }
    )
    monkeypatch.setattr(find_var_module, "settings", fake_settings)
    monkeypatch.setattr(find_var_module, "klass", SimpleNamespace())

    result = find_vars(["keep", "missing"])

    assert result["keep"] is not None
    assert result["keep"]["name"] == "keep"
    assert result["missing"] is None


def test_variables_missing_from_config(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        find_var_module, "settings", FakeSettings(variables={"a": {}, "b": {}})
    )

    assert variables_missing_from_config(["a", "c"]) == ["c"]
