import importlib

import pandas as pd
import pytest

get_dtypes_module = importlib.import_module(
    "nudb_use.metadata.nudb_config.map_get_dtypes"
)
from nudb_use.metadata.nudb_config.map_get_dtypes import cast_to_preferred_dtypes
from nudb_use.metadata.nudb_config.map_get_dtypes import get_dtype_from_dict
from nudb_use.metadata.nudb_config.map_get_dtypes import (
    get_dtypes as get_dtypes_function,
)
from nudb_use.metadata.nudb_config.map_get_dtypes import map_dtype_datadoc


class DummyVar:
    def __init__(self, dtype: str, renamed_from: str | list[str] | None = None):
        self.dtype = dtype
        self.renamed_from = renamed_from

    def __getitem__(self, key: str) -> str | list[str] | None:
        """Get item from the DummyVar."""
        type_narrowed: str | list[str] | None = getattr(self, key)
        return type_narrowed


def test_get_dtype_from_dict_overrides_datetimes() -> None:
    mapping = {"DATETIME": "datetime64[s]", "STRING": "string[pyarrow]"}

    assert (
        get_dtype_from_dict("datetime", mapping, datetimes_as_string=True)
        == "string[pyarrow]"
    )
    assert get_dtype_from_dict("DATETIME", mapping) == "datetime64[s]"


def test_get_dtype_from_dict_unknown_type() -> None:
    with pytest.raises(ValueError, match="Unkown type: UNKNOWN"):
        get_dtype_from_dict("unknown", {"STRING": "string"})


def test_map_dtype_datadoc_unknown_engine() -> None:
    with pytest.raises(KeyError):
        map_dtype_datadoc("STRING", engine="spark")


def test_get_dtypes_maps_missing_and_renamed(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_vars = {
        "new_col": DummyVar("STRING"),
        "date_col": DummyVar("DATETIME", renamed_from="old_date"),
        "num_col": DummyVar("INTEGER", renamed_from=["old_num1", "old_num2"]),
    }
    monkeypatch.setattr(get_dtypes_module, "SETTINGS", {"variables": fake_vars})

    result = get_dtypes_function(
        ["missing", "new_col", "old_date", "old_num1"], datetimes_as_string=True
    )

    assert result == {
        "missing": None,
        "new_col": "string[pyarrow]",
        "old_date": "string[pyarrow]",
        "old_num1": "Int64",
    }


def test_cast_to_preferred_dtypes() -> None:

    test_data = pd.DataFrame(
        {
            "x": pd.Series([1, 2, 3], dtype="int64"),
            "z": pd.Series([3.14, 0.7218, 0.070400], dtype="float64"),
            "y": pd.Series([True, True, False], dtype="bool"),
            "w": pd.Series(["Hello", "there", "!"], dtype="str"),
            "g": pd.to_datetime(
                pd.Series(["2020-01-01", "2021-01-01", "2022-01-01"])
            ).astype("datetime64[ns]"),
        }
    )

    casted = cast_to_preferred_dtypes(test_data)

    assert casted["x"].dtype == "Int64"
    assert casted["z"].dtype == "Float64"
    assert casted["y"].dtype == "bool[pyarrow]"
    assert casted["w"].dtype == "string[pyarrow]"
    assert casted["g"].dtype == "datetime64[s]"
