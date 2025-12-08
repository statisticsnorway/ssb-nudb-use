from types import SimpleNamespace
from typing import Any

import pandas as pd

from nudb_use.metadata.nudb_config.variable_names import update_colnames
from nudb_use.metadata.nudb_klass import codes
from nudb_use.variables import checks
from nudb_use.variables.checks import check_cols_against_klass_codelists
from nudb_use.variables.checks import check_column_presence
from nudb_use.variables.checks import identify_cols_not_in_keep_drop_in_paths
from nudb_use.variables.checks import pyarrow_columns_from_metadata


def test_pyarrow_columns_from_metadata(tmp_path: Any, avslutta: pd.DataFrame) -> None:
    # Setup file on disk
    file_path = tmp_path / "avslutta_tmp_pyarrow.parquet"
    avslutta.to_parquet(file_path)
    assert "fnr" in pyarrow_columns_from_metadata(file_path)


def test_identify_cols_not_in_keep_drop_in_paths(
    tmp_path: Any, avslutta: pd.DataFrame
) -> None:
    # Setup file on disk
    file_path = tmp_path / "avslutta_tmp_cols_not_in_keep_drop.parquet"
    avslutta.to_parquet(file_path)

    cols_keep = ["fnr"]
    cols_drop = ["snr"]
    missing_cols = identify_cols_not_in_keep_drop_in_paths(
        paths=[file_path],
        cols_keep=cols_keep,
        cols_drop=cols_drop,
        raise_error_found=False,
    )

    # We should now have many columns not defined in fnr or snr
    assert len(missing_cols)
    assert "estart" in missing_cols


def test_check_column_presence_no_errors(avslutta: pd.DataFrame) -> None:
    avslutta_new_colnames = update_colnames(avslutta, "avslutta")
    no_errors = check_column_presence(
        avslutta_new_colnames,
        check_for=list(avslutta_new_colnames.columns),
        raise_errors=False,
    )
    assert no_errors == []


def test_check_column_presence_has_errors(avslutta: pd.DataFrame) -> None:
    has_errors = check_column_presence(avslutta, "igang", raise_errors=False)
    # There are errors
    assert len(has_errors)
    # They are children of the base exception class
    assert all(isinstance(x, Exception) for x in has_errors)


def test_check_cols_against_klass_codelists(
    avslutta: pd.DataFrame, caplog: Any, monkeypatch: Any
) -> None:
    class FakeCodes(dict[str, str]):
        def to_dict(self) -> dict[str, str]:
            return {"ok": "ok"}

    class FakeKlassClassification:
        def __init__(self, _klass_id: int):
            self._klass_id = _klass_id
            self.versions: list[dict[str, str]] = []

        def get_codes(
            self, from_date: str | None = None, to_date: str | None = None
        ) -> FakeCodes:
            return FakeCodes()

    # Avoid network calls by faking klass lookups
    monkeypatch.setattr(
        checks,
        "klass",
        SimpleNamespace(
            KlassClassification=FakeKlassClassification,
            KlassVariant=lambda _id: SimpleNamespace(data={"code": []}),
        ),
    )
    monkeypatch.setattr(
        codes,
        "find_earliest_latest_klass_version_date",
        lambda _id: ("2000-01-01", "2001-01-01"),
    )

    check_cols_against_klass_codelists(avslutta)
