import pandas as pd
import pytest

from nudb_use.exceptions.exception_classes import NudbQualityError
from nudb_use.quality.check_drop_cols import check_drop_cols_for_valid_cols


def test_overlapping_drop_valid_cols() -> None:
    with pytest.raises(NudbQualityError):
        check_drop_cols_for_valid_cols(["snr"], raise_errors=True)


def test_overlapping_drop_invalid_cols() -> None:
    errs = check_drop_cols_for_valid_cols(
        ["random_col_1", "random_col_2"], raise_errors=False
    )
    assert errs is None


def test_drop_derivable_cols_ok() -> None:
    errs = check_drop_cols_for_valid_cols(["snr_mrk"], raise_errors=False)
    assert errs is None


import importlib
from types import SimpleNamespace


def test_drop_dataset_rename_warning(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    module = importlib.import_module("nudb_use.quality.check_drop_cols")
    monkeypatch.setattr(
        module,
        "get_var_metadata",
        lambda: pd.DataFrame(
            {
                "unit": ["valid", "valid", "utdatert"],
                "renamed_from": [["old"], None, None],
            },
            index=["current", "other", "retired"],
        ),
    )
    monkeypatch.setattr(
        module,
        "settings",
        SimpleNamespace(
            datasets={
                "ongoing": SimpleNamespace(
                    dataset_specific_renames={"ftype": "current", "obsolete": "retired"}
                ),
                "completed": SimpleNamespace(
                    dataset_specific_renames={"ftype": "other"}
                ),
            }
        ),
    )
    assert check_drop_cols_for_valid_cols(["FTYPE", "old", "obsolete"]) is None
    warning = caplog.text
    assert all(
        text in warning
        for text in ["FTYPE", "old", "current", "other", "ongoing", "completed"]
    )
    assert "obsolete" not in warning
    caplog.clear()
    check_drop_cols_for_valid_cols(["ftype"], dataset_name="ongoing")
    assert "ongoing" in caplog.text and "completed" not in caplog.text
