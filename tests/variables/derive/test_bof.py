from typing import Any

import pandas as pd

from nudb_use.variables.derive import bof as bof_module


def test_bof_eierforhold_preserves_index_and_falls_back_on_orgnrbed(
    monkeypatch: Any,
) -> None:
    catalogue = pd.DataFrame(
        {
            "orgnr_foretak": ["111", "999", "999"],
            "orgnrbed": ["bed-a", "bed-b", "bed-c"],
            "bof_eierforhold": ["1", "4", "5"],
        }
    )

    class FakeNudbData:
        def __init__(self, _name: str) -> None:
            pass

        def select(self, _cols: str) -> "FakeNudbData":
            return self

        def where(self, _expr: str) -> "FakeNudbData":
            return self

        def df(self) -> pd.DataFrame:
            return catalogue

    monkeypatch.setattr(bof_module, "NudbData", FakeNudbData)

    df = pd.DataFrame(
        {
            "orgnr_foretak": ["111", "222", "333"],
            "orgnrbed": ["bed-a", "bed-c", "bed-missing"],
        },
        index=[10, 20, 30],
    )

    result = bof_module.bof_eierforhold(df.copy())

    assert result.index.tolist() == [10, 20, 30]
    pd.testing.assert_series_equal(
        result["bof_eierforhold"],
        pd.Series(["1", "5", pd.NA], index=[10, 20, 30], name="bof_eierforhold"),
        check_dtype=False,
    )
