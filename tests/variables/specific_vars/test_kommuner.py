from types import SimpleNamespace
from typing import Any

import pandas as pd
import pytest

from nudb_use.variables.specific_vars import kommuner
from nudb_use.variables.specific_vars.kommuner import correct_kommune_single_values
from nudb_use.variables.specific_vars.kommuner import keep_only_valid_kommune_codes


def test_keep_only_valid_kommune_codes(monkeypatch: Any) -> None:
    class FakeCodes(dict[str, str]):
        def to_dict(self) -> dict[str, str]:
            return {"0301": "Oslo", "1103": "Stavanger"}

    class FakeKlassClassification:
        def __init__(self, _klass_id: int) -> None:
            self._klass_id = _klass_id

        def get_codes(self, from_date: str, to_date: str) -> FakeCodes:
            return FakeCodes()

    monkeypatch.setattr(
        kommuner,
        "klass",
        SimpleNamespace(KlassClassification=FakeKlassClassification),
    )

    input_series = pd.Series(
        [
            "0301",
            "1103",
            "1100",
            "1199",
            "9999",
            "1234",
            "2580",
            "2111",
            "9900",
        ]
    )

    result = keep_only_valid_kommune_codes(
        input_series, from_date="2000-01-01", to_date="2000-12-31"
    )

    assert result[0] == "0301"
    assert result[1] == "1103"
    assert result[2] == "1100"
    assert result[3] == "1100"
    assert pd.isna(result[4])
    assert pd.isna(result[5])
    assert result[6] == "2580"
    assert result[7] == "2111"
    assert pd.isna(result[8])


def test_correct_kommune_single_values() -> None:
    df = pd.DataFrame(
        {
            "utd_skolekom": [
                "0300",
                "2100",
                "2500",
                "2400",
                "9900",
                "9998",
                "0000",
                pd.NA,
                "1234",
            ]
        }
    )

    result = correct_kommune_single_values(df, col_name="utd_skolekom")

    assert result["utd_skolekom"].tolist() == [
        "0301",
        "2111",
        "2580",
        "2580",
        "9999",
        "9999",
        "9999",
        "9999",
        "1234",
    ]


def test_correct_kommune_single_values_raises_on_weird_codes() -> None:
    df = pd.DataFrame({"utd_skolekom": ["123", "12a4", "12345"]})

    with pytest.raises(ValueError):
        correct_kommune_single_values(df, col_name="utd_skolekom")
