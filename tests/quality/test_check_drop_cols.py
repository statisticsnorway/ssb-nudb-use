import pandas as pd
import pytest

from nudb_use.exceptions.exception_classes import NudbQualityError
from nudb_use.quality.check_drop_cols import check_drop_cols_for_valid_cols


def test_overlapping_drop_valid_cols(avslutta: pd.DataFrame) -> None:
    cols = list(avslutta.columns[0:4])
    with pytest.raises(NudbQualityError):
        check_drop_cols_for_valid_cols(cols, raise_errors=True)


def test_overlapping_drop_invalid_cols() -> None:
    errs = check_drop_cols_for_valid_cols(
        ["random_col_1", "random_col_2"], raise_errors=False
    )
    assert errs is None


def test_drop_derivable_cols_ok() -> None:
    errs = check_drop_cols_for_valid_cols(["snr_mrk"], raise_errors=False)
    assert errs is None
