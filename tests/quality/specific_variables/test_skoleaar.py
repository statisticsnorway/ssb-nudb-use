import pandas as pd

from nudb_use.exceptions.exception_classes import NudbQualityError
from nudb_use.quality.specific_variables.skoleaar import check_skoleaar
from nudb_use.quality.specific_variables.skoleaar import (
    check_skoleaar_contains_one_year,
)
from nudb_use.quality.specific_variables.skoleaar import (
    check_skoleaar_contains_sane_years,
)
from nudb_use.quality.specific_variables.skoleaar import (
    check_skoleaar_contains_two_years_one_offset,
)
from nudb_use.quality.specific_variables.skoleaar import check_skoleaar_is_string_dtype


def test_check_skoleaar_no_skoleaar_cols() -> None:
    df = pd.DataFrame({"other_col": ["2020", "2021"]})

    errors = check_skoleaar(df)

    assert errors == []


def test_check_skoleaar_non_string_dtype() -> None:
    df = pd.DataFrame({"utd_skoleaar_start": pd.Series([2020, 2021], dtype="Int64")})

    errors = check_skoleaar(df)

    assert len(errors) == 1
    assert isinstance(errors[0], NudbQualityError)
    assert "string dtype" in str(errors[0])


def test_check_skoleaar_valid_single_years() -> None:
    df = pd.DataFrame(
        {"utd_skoleaar_start": pd.Series(["1970", "2050"], dtype="string")}
    )

    errors = check_skoleaar(df)

    assert errors == []


def test_check_skoleaar_is_string_dtype_valid() -> None:
    col = pd.Series(["2000", "2001"], dtype="string")

    err = check_skoleaar_is_string_dtype("utd_skoleaar_start", col)

    assert err is None


def test_check_skoleaar_contains_one_year_invalid() -> None:
    col = pd.Series(["1999", "19992000"], dtype="string")

    err = check_skoleaar_contains_one_year("utd_skoleaar_start", col)

    assert isinstance(err, NudbQualityError)
    assert "single year" in str(err)


def test_check_skoleaar_contains_sane_years_invalid() -> None:
    col = pd.Series(["2051"], dtype="string")

    err = check_skoleaar_contains_sane_years("utd_skoleaar_start", col)

    assert isinstance(err, NudbQualityError)
    assert "values between" in str(err)


def test_check_skoleaar_contains_two_years_one_offset_invalid() -> None:
    col = pd.Series(["19992001", "1999ABCD"], dtype="string")

    err = check_skoleaar_contains_two_years_one_offset("utd_skoleaar", col)

    assert isinstance(err, NudbQualityError)
    assert "WEIRD OFFSETS" in str(err)
