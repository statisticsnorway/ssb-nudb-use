import pandas as pd
import pytest

from nudb_use.exceptions.exception_classes import NudbQualityError
from nudb_use.quality.thresholds import filled_value_to_threshold
from nudb_use.quality.thresholds import non_empty_to_threshold


def test_filled_value_to_threshold(avslutta: pd.DataFrame) -> None:
    col = avslutta.columns[0]
    series = avslutta[col].head(4).copy()
    match_value = series.iloc[0]

    assert filled_value_to_threshold(series, match_value, 0, raise_error=False) is None

    series.iloc[:] = pd.NA
    with pytest.raises(NudbQualityError):
        filled_value_to_threshold(series, match_value, 50, raise_error=True)

    err = filled_value_to_threshold(series, match_value, 50, raise_error=False)
    assert err is not None
    assert "below the threshold" in str(err)


def test_non_empty_to_threshold(avslutta: pd.DataFrame) -> None:
    col = avslutta.columns[0]
    series = avslutta[col].head(4).copy()

    assert non_empty_to_threshold(series, 0, raise_error=False) is None

    series.iloc[:] = pd.NA
    with pytest.raises(NudbQualityError):
        non_empty_to_threshold(series, 50, raise_error=True)

    err = non_empty_to_threshold(series, 50, raise_error=False)
    assert err is not None
    assert "below the threshold" in str(err)
