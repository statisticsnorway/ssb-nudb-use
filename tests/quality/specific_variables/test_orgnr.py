from typing import Any

import pandas as pd

import nudb_use.quality.specific_variables.orgnr as orgnr_quality
from nudb_use.exceptions.exception_classes import NudbQualityError
from nudb_use.quality.specific_variables.orgnr import check_orgnr_foretak
from nudb_use.quality.specific_variables.orgnr import check_orgnrbed
from nudb_use.quality.specific_variables.orgnr import check_outdated_orgnr_cols
from nudb_use.quality.specific_variables.orgnr import (
    subcheck_col_contains_invalid_orgnr,
)
from nudb_use.quality.specific_variables.orgnr import (
    subcheck_orgnrbed_orgnr_foretak_connected,
)

FORETAK_VALUES = {
    "100000001",
    "100000002",
    "100000003",
    "100000099",
}
ORGNRBED_VALUES = {
    "200000001",
    "200000002",
    "200000003",
    "200000099",
}


def _stub_split_orgnr_col(
    col: pd.Series,
    put_invalid_in_orgnr_foretak: bool = True,
) -> tuple[pd.Series, pd.Series]:
    orgnr_foretak = pd.Series(pd.NA, index=col.index, dtype="string")
    orgnrbed = pd.Series(pd.NA, index=col.index, dtype="string")

    orgnr_foretak.loc[col.isin(FORETAK_VALUES)] = col
    orgnrbed.loc[col.isin(ORGNRBED_VALUES)] = col

    if put_invalid_in_orgnr_foretak:
        orgnr_foretak.loc[col.notna() & orgnr_foretak.isna() & orgnrbed.isna()] = col

    return orgnr_foretak, orgnrbed


def test_check_outdated_orgnr_cols_reports_old_names() -> None:
    df = pd.DataFrame({"org_nr": ["100000001"], "keep": [1]})

    errors = check_outdated_orgnr_cols(df)

    assert len(errors) == 1
    assert isinstance(errors[0], NudbQualityError)
    assert "org_nr" in str(errors[0])
    assert "orgnr_foretak" in str(errors[0])


def test_subcheck_col_contains_invalid_orgnr_reports_unknown_values(
    monkeypatch: Any,
) -> None:
    monkeypatch.setattr(orgnr_quality, "_split_orgnr_col", _stub_split_orgnr_col)

    err = subcheck_col_contains_invalid_orgnr(
        pd.Series(["100000001", "999999999", pd.NA], dtype="string"),
        "orgnr_foretak",
    )

    assert isinstance(err, NudbQualityError)
    assert "999999999" in str(err)


def test_check_orgnr_foretak_reports_orgnrbed_values(
    monkeypatch: Any,
) -> None:
    monkeypatch.setattr(orgnr_quality, "_split_orgnr_col", _stub_split_orgnr_col)
    df = pd.DataFrame(
        {
            "orgnr_foretak": pd.Series(
                ["100000001", "200000099"],
                dtype="string",
            )
        }
    )

    errors = check_orgnr_foretak(df, use_external_datasets=True)

    assert len(errors) == 1
    assert isinstance(errors[0], NudbQualityError)
    assert "orgnrbed-values" in str(errors[0])
    assert "200000099" in str(errors[0])


def test_check_orgnrbed_reports_orgnr_foretak_values(
    monkeypatch: Any,
) -> None:
    monkeypatch.setattr(orgnr_quality, "_split_orgnr_col", _stub_split_orgnr_col)
    df = pd.DataFrame(
        {
            "orgnrbed": pd.Series(
                ["200000001", "100000099"],
                dtype="string",
            )
        }
    )

    errors = check_orgnrbed(df, use_external_datasets=True)

    assert len(errors) == 1
    assert isinstance(errors[0], NudbQualityError)
    assert "orgnr_foretak-values" in str(errors[0])
    assert "100000099" in str(errors[0])


def test_subcheck_orgnrbed_orgnr_foretak_connected_reports_missing_bof_pairs(
    monkeypatch: Any,
) -> None:
    bof_connections = pd.DataFrame(
        {
            "orgnr": ["100000001", "100000002"],
            "orgnrbed": ["200000001", "200000002"],
        },
        dtype="string",
    )

    class FakeNudbData:
        def __init__(self, name: str) -> None:
            assert name == "_bof_dated_orgnr_connections"

        def select(self, expr: str) -> "FakeNudbData":
            assert expr == "DISTINCT orgnr, orgnrbed"
            return self

        def where(self, expr: str) -> "FakeNudbData":
            assert "orgnr in" in expr
            assert "orgnrbed in" in expr
            return self

        def df(self) -> pd.DataFrame:
            return bof_connections.copy()

    monkeypatch.setattr(orgnr_quality, "NudbData", FakeNudbData)

    err = subcheck_orgnrbed_orgnr_foretak_connected(
        col_foretak=pd.Series(
            ["100000001", "100000002", "100000003"],
            dtype="string",
        ),
        col_foretak_name="orgnr_foretak",
        col_orgnrbed=pd.Series(
            ["200000001", "200000002", "200000003"],
            dtype="string",
        ),
        col_orgnrbed_name="orgnrbed",
    )

    assert isinstance(err, NudbQualityError)
    err_msg = str(err)
    assert "Found 1 orgnr_foretak/orgnrbed-connections" in err_msg
    assert "100000003" in err_msg
    assert "200000003" in err_msg
    assert "100000001" not in err_msg
    assert "200000001" not in err_msg


def test_check_orgnrbed_reports_invalid_connections_with_stubbed_bof(
    monkeypatch: Any,
) -> None:
    monkeypatch.setattr(orgnr_quality, "_split_orgnr_col", _stub_split_orgnr_col)

    bof_connections = pd.DataFrame(
        {
            "orgnr": ["100000001", "100000002"],
            "orgnrbed": ["200000001", "200000002"],
        },
        dtype="string",
    )

    class FakeNudbData:
        def __init__(self, name: str) -> None:
            assert name == "_bof_dated_orgnr_connections"

        def select(self, expr: str) -> "FakeNudbData":
            return self

        def where(self, expr: str) -> "FakeNudbData":
            return self

        def df(self) -> pd.DataFrame:
            return bof_connections.copy()

    monkeypatch.setattr(orgnr_quality, "NudbData", FakeNudbData)

    df = pd.DataFrame(
        {
            "orgnr_foretak": pd.Series(
                ["100000001", "100000002", "100000003"],
                dtype="string",
            ),
            "orgnrbed": pd.Series(
                ["200000001", "200000003", "200000002"],
                dtype="string",
            ),
        }
    )

    errors = check_orgnrbed(df, use_external_datasets=True)

    assert len(errors) == 1
    err_msg = str(errors[0])
    assert "Found 2 orgnr_foretak/orgnrbed-connections" in err_msg
    assert "100000002" in err_msg
    assert "200000003" in err_msg
    assert "100000003" in err_msg
    assert "200000002" in err_msg
