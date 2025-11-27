import pandas as pd
import pytest

from nudb_use.quality.widths import check_column_widths


def test_check_column_widths() -> None:
    df = pd.DataFrame({"code": ["aa", "bb", "cc"]})

    assert check_column_widths(df, {"code": [2]}, raise_errors=False) == []

    df.loc[0, "code"] = "x"
    with pytest.raises(ExceptionGroup):
        check_column_widths(df, {"code": [2]}, raise_errors=True)

    errors = check_column_widths(df, {"code": [2]}, raise_errors=False)
    assert len(errors) == 1
    assert "code" in str(errors[0])


def test_check_column_widths_from_config(monkeypatch: pytest.MonkeyPatch) -> None:
    df = pd.DataFrame({"code": ["abc", "def", pd.NA]})

    class DummyVar(dict[str, list[int]]):
        def __init__(self, length: list[int]):
            super().__init__(length=length)
            self.length = length

    dummy_settings = type(
        "DummySettings",
        (),
        {"variables": {"code": DummyVar([3])}},
    )()
    monkeypatch.setattr("nudb_use.quality.widths.settings", dummy_settings)

    # Passing None triggers config lookup
    assert check_column_widths(df, widths=None, raise_errors=False) == []
