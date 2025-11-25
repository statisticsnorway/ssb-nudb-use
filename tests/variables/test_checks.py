from types import SimpleNamespace

from nudb_use.metadata.nudb_config.variable_names import update_colnames
from nudb_use.variables import checks
from nudb_use.variables.checks import (
    pyarrow_columns_from_metadata,
    identify_cols_not_in_keep_drop_in_paths,
    check_column_presence,
    check_cols_against_klass_codelists,
)


def test_pyarrow_columns_from_metadata(tmp_path, avslutta) -> None:
    # Setup file on disk
    file_path = tmp_path / "avslutta_tmp_pyarrow.parquet"
    avslutta.to_parquet(file_path)
    assert "fnr" in pyarrow_columns_from_metadata(file_path)


def test_identify_cols_not_in_keep_drop_in_paths(tmp_path, avslutta) -> None:
    # Setup file on disk
    file_path = tmp_path / "avslutta_tmp_cols_not_in_keep_drop.parquet"
    avslutta.to_parquet(file_path)

    cols_keep = ["fnr"]
    cols_drop = ["pers_id"]
    missing_cols = identify_cols_not_in_keep_drop_in_paths(
        paths=[file_path],
        cols_keep=cols_keep,
        cols_drop=cols_drop,
        raise_error_found=False)
    
    # We should now have many columns not defined in fnr or pers_id
    assert len(missing_cols)
    assert "estart" in missing_cols


def test_check_column_presence_no_errors(avslutta) -> None:
    avslutta_new_colnames = update_colnames(avslutta, "avslutta")
    no_errors = check_column_presence(avslutta_new_colnames, check_for=avslutta_new_colnames.columns, raise_errors=False)
    assert no_errors == []

def test_check_column_presence_has_errors(avslutta) -> None:
    has_errors = check_column_presence(avslutta, "igang", raise_errors=False)
    # There are errors
    assert len(has_errors)
    # They are children of the base exception class
    assert all(isinstance(x, Exception) for x in has_errors)


def test_check_cols_against_klass_codelists(avslutta, caplog, monkeypatch) -> None:
    class FakeCodes(dict):
        def to_dict(self):
            return {"ok": "ok"}

    class FakeKlassClassification:
        def __init__(self, _klass_id):
            self._klass_id = _klass_id
            self.versions = []

        def get_codes(self, from_date=None, to_date=None):
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
        checks,
        "_find_earliest_latest_klass_version_date",
        lambda _id: ("2000-01-01", "2001-01-01"),
    )

    assert check_cols_against_klass_codelists(avslutta) is None
