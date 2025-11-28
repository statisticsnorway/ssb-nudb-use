import pandas as pd

from nudb_use.exceptions.exception_classes import NudbQualityError
from nudb_use.quality.specific_variables.vg_fullfoertkode_detaljert import (
    check_vg_fullfoertkode_detaljert,
)


def test_check_vg_fullfoertkode_detaljert_valid(avslutta: pd.DataFrame) -> None:
    df = avslutta.head(3).copy()
    df["utd_utdanningstype"] = ["211", "212", "610"]
    df["vg_fullfoertkode_detaljert"] = ["a", "b", "c"]

    errors = check_vg_fullfoertkode_detaljert(df)
    assert errors == []


def test_check_vg_fullfoertkode_detaljert_invalid(avslutta: pd.DataFrame) -> None:
    df = avslutta.head(2).copy()
    df["utd_utdanningstype"] = ["999", "220"]
    df["vg_fullfoertkode_detaljert"] = ["bad", pd.NA]

    errors = check_vg_fullfoertkode_detaljert(df)
    assert len(errors) == 1
    assert isinstance(errors[0], NudbQualityError)
    assert "should be empty" in str(errors[0])


def test_check_vg_fullfoertkode_detaljert_missing_cols(avslutta: pd.DataFrame) -> None:
    df = avslutta.head(1).copy()
    df = df.drop(
        columns=[
            col
            for col in df.columns
            if col in {"utd_utdanningstype", "vg_fullfoertkode_detaljert"}
        ]
    )

    errors = check_vg_fullfoertkode_detaljert(df)
    assert errors == []
