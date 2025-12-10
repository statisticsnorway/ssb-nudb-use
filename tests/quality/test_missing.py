import pandas as pd
import pytest

import nudb_use.quality.missing as missing
from nudb_use.quality.missing import check_columns_only_missing
from nudb_use.quality.missing import check_missing_thresholds_dataset_name
from nudb_use.quality.missing import check_non_missing
from nudb_use.quality.missing import df_within_missing_thresholds
from nudb_use.quality.missing import empty_percents_over_columns
from nudb_use.quality.missing import get_thresholds_from_config
from nudb_use.quality.missing import last_period_within_thresholds


def test_check_non_missing(avslutta: pd.DataFrame) -> None:
    col = avslutta.columns[0]

    assert check_non_missing(avslutta, [col], raise_errors=False) == []

    avslutta.loc[0, col] = pd.NA
    with pytest.raises(ExceptionGroup):
        check_non_missing(avslutta, [col], raise_errors=True)

    errors = check_non_missing(avslutta, [col], raise_errors=False)
    assert len(errors) == 1
    assert col in str(errors[0])


def test_check_columns_only_missing(avslutta: pd.DataFrame) -> None:
    assert check_columns_only_missing(avslutta, raise_errors=False) == []

    # Get two rows from the fixture
    empty_df = avslutta.head(2).copy()
    # Set all cells to missing
    empty_df.loc[:, :] = pd.NA
    # If any columns are all empty, this should raise an error
    with pytest.raises(ExceptionGroup):
        check_columns_only_missing(empty_df, raise_errors=True)

    errors = check_columns_only_missing(empty_df, raise_errors=False)
    # The amount of errors should match the amount of columns
    assert len(errors) == len(empty_df.columns)


def test_empty_percents_over_columns(avslutta: pd.DataFrame) -> None:
    sample = avslutta.iloc[:4, :2].copy()
    col_a, col_b = sample.columns
    sample.loc[0, col_a] = pd.NA
    sample[col_b] = pd.NA

    result = empty_percents_over_columns(sample)

    assert set(result.columns) == {col_a, col_b}
    assert result[col_a].iloc[0] == pytest.approx(25.0)
    assert result[col_b].iloc[0] == pytest.approx(100.0)


def test_last_period_within_thresholds(avslutta: pd.DataFrame) -> None:
    col = avslutta.columns[0]
    df = avslutta[[col]].head(4).copy()
    df["period"] = [2020, 2020, 2021, 2021]
    df.loc[df["period"] == 2021, col] = pd.NA

    thresholds = {col: 0.0}
    with pytest.raises(ExceptionGroup):
        last_period_within_thresholds(df, "period", thresholds, raise_errors=True)

    errors = last_period_within_thresholds(df, "period", thresholds, raise_errors=False)
    assert len(errors) == 1
    assert col in str(errors[0])


def test_df_within_missing_thresholds(avslutta: pd.DataFrame) -> None:
    col = avslutta.columns[0]
    df = avslutta[[col]].head(2).copy()
    df.loc[0, col] = pd.NA

    with pytest.raises(ExceptionGroup):
        df_within_missing_thresholds(df, {col: 10.0}, raise_errors=True)

    errors = df_within_missing_thresholds(df, {col: 10.0}, raise_errors=False)
    assert len(errors) == 1
    assert col in str(errors[0])

    assert df_within_missing_thresholds(df, None, raise_errors=False) == []


def test_check_missing_thresholds_dataset_name(
    avslutta: pd.DataFrame, monkeypatch: pytest.MonkeyPatch
) -> None:
    col = avslutta.columns[0]
    df = avslutta[[col]].head(2).copy()
    df[col] = pd.NA

    monkeypatch.setattr(missing, "get_thresholds_from_config", lambda name: {col: 0.0})
    with pytest.raises(ExceptionGroup):
        check_missing_thresholds_dataset_name(df, "avslutta", raise_errors=True)

    errors = check_missing_thresholds_dataset_name(df, "avslutta", raise_errors=False)
    assert len(errors) == 1

    monkeypatch.setattr(missing, "get_thresholds_from_config", lambda name: {})
    assert (
        check_missing_thresholds_dataset_name(df, "avslutta", raise_errors=True) == []
    )


def test_get_thresholds_from_config(
    avslutta: pd.DataFrame, monkeypatch: pytest.MonkeyPatch
) -> None:
    col = avslutta.columns[0]
    dummy_thresholds = {col: 5.0}
    dummy_settings = type(
        "DummySettings",
        (),
        {"datasets": {"avslutta": {"thresholds_empty": dummy_thresholds}}},
    )()

    monkeypatch.setattr(missing, "settings", dummy_settings)

    assert get_thresholds_from_config("avslutta") == dummy_thresholds
