import pandas as pd
import pytest

from nudb_use.quality.values import get_fill_amount_per_column
from nudb_use.quality.values import values_not_in_column


def test_get_fill_amount_per_column(avslutta: pd.DataFrame) -> None:
    sample: pd.DataFrame = avslutta.iloc[:4, :2].copy()
    first, second = sample.columns

    sample.loc[0, first] = pd.NA
    sample.loc[[0, 1], second] = pd.NA

    result = get_fill_amount_per_column(sample)

    assert result[first] == pytest.approx(75.0)
    assert result[second] == pytest.approx(50.0)


def test_values_not_in_column(avslutta: pd.DataFrame) -> None:
    df = avslutta.head(3).copy()
    df["status"] = ["ok", "ok", "bad"]

    assert values_not_in_column(df["status"], "forbidden", raise_error=False) is None

    with pytest.raises(ValueError):
        values_not_in_column(df["status"], "bad", raise_error=True)

    err = values_not_in_column(df["status"], "bad", raise_error=False)
    assert isinstance(err, ValueError)
    assert "Values in col" in str(err)
