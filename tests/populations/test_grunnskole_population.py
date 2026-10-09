import pandas as pd
import pytest

from nudb_use.populations.grunnskole_population import _add_age_at_school_start
from nudb_use.populations.grunnskole_population import (
    _create_boolean_vars_for_exclusion,
)
from nudb_use.populations.grunnskole_population import _derive_har_grunnskolenaering
from nudb_use.populations.grunnskole_population import _exclude_to_grunnskole_population
from nudb_use.populations.grunnskole_population import (
    _validate_required_cols_for_exclusion,
)


def test_validate_required_cols_for_exclusion_passes() -> None:
    df = pd.DataFrame(
        {
            "a": [1],
            "b": [2],
        }
    )

    _validate_required_cols_for_exclusion(df, ["a", "b"])


def test_validate_required_cols_for_exclusion_raises() -> None:
    df = pd.DataFrame({"a": [1]})

    with pytest.raises(
        ValueError,
        match="Missing required columns: b",
    ):
        _validate_required_cols_for_exclusion(df, ["a", "b"])


def test_derive_har_grunnskolenaering() -> None:
    df = pd.DataFrame(
        {
            "bof_naering1_sn2025": [
                "85.201",
                "85.310",
                None,
            ],
            "bof_naering2_sn2025": [
                None,
                "85.202",
                None,
            ],
            "bof_naering3_sn2025": [
                None,
                None,
                "99.999",
            ],
        }
    )

    result = _derive_har_grunnskolenaering(df)

    assert result["har_grunnskolenaering"].tolist() == [
        True,
        True,
        False,
    ]


def test_derive_har_grunnskolenaering_missing_columns() -> None:
    df = pd.DataFrame(
        {
            "bof_naering1_sn2025": ["85.201"],
        }
    )

    with pytest.raises(
        ValueError,
        match="Missing required columns",
    ):
        _derive_har_grunnskolenaering(df)


def test_add_age_at_school_start() -> None:
    df = pd.DataFrame(
        {
            "pers_foedselsdato": pd.to_datetime(
                [
                    "2008-05-01",
                    "2007-11-01",
                ]
            )
        }
    )

    result = _add_age_at_school_start(
        df=df,
        start_year=2024,
    )

    assert result["pers_alder"].tolist() == [
        16,
        17,
    ]


def test_add_age_at_school_start_keeps_existing_column() -> None:
    df = pd.DataFrame(
        {
            "pers_alder": [15],
        }
    )

    result = _add_age_at_school_start(
        df=df,
        start_year=2024,
    )

    assert result["pers_alder"].tolist() == [15]


def test_add_age_at_school_start_missing_birthdate() -> None:
    df = pd.DataFrame(
        {
            "person_id": [1],
        }
    )

    with pytest.raises(
        ValueError,
        match="Missing required columns: pers_foedselsdato",
    ):
        _add_age_at_school_start(
            df=df,
            start_year=2024,
        )


def test_create_boolean_vars_for_exclusion() -> None:
    df = pd.DataFrame(
        {
            "har_grunnskolenaering": [True, False],
            "gro_skolenavn_inn": [
                "Oslo skole",
                "Steinerskolen",
            ],
            "utd_skolekom": [
                "0301",
                "2599",
            ],
            "orgnrbed": [
                "123",
                None,
            ],
            "pers_alder": [
                15,
                17,
            ],
            "gro_elevstatus": [
                "E",
                "X",
            ],
            "gr_grunnskolepoeng": [
                40,
                0,
            ],
        }
    )

    result = _create_boolean_vars_for_exclusion(df)

    assert result["er_ikke_grunnskolenaering"].tolist() == [
        False,
        True,
    ]

    assert result["er_steinerskole"].tolist() == [
        False,
        True,
    ]

    assert result["er_norskskoleiutlandet"].tolist() == [
        False,
        True,
    ]

    assert result["er_ukjentorgnrbed"].tolist() == [
        False,
        True,
    ]

    assert result["er_over16aar"].tolist() == [
        False,
        True,
    ]

    assert result["er_ikke_elevstatus_es"].tolist() == [
        False,
        True,
    ]

    assert result["har_ikke_grunnskolepoeng"].tolist() == [
        False,
        True,
    ]


def test_create_boolean_vars_for_exclusion_without_res() -> None:
    df = pd.DataFrame(
        {
            "har_grunnskolenaering": [True],
            "gro_skolenavn_inn": ["Oslo skole"],
            "utd_skolekom": ["0301"],
            "orgnrbed": ["123"],
            "pers_alder": [15],
            "gro_elevstatus": [None],
        }
    )

    result = _create_boolean_vars_for_exclusion(
        df,
        res=False,
    )

    assert "har_ikke_grunnskolepoeng" not in result.columns


def test_create_boolean_vars_for_exclusion_missing_elevstatus() -> None:
    df = pd.DataFrame(
        {
            "har_grunnskolenaering": [True],
            "gro_skolenavn_inn": ["Oslo skole"],
            "utd_skolekom": ["0301"],
            "orgnrbed": ["123"],
            "pers_alder": [15],
            "gro_elevstatus": [None],
            "gr_grunnskolepoeng": [40],
        }
    )

    result = _create_boolean_vars_for_exclusion(df)

    assert not result["er_ikke_elevstatus_es"].iloc[0]


def test_exclude_to_grunnskole_population() -> None:
    df = pd.DataFrame(
        {
            "er_ikke_grunnskolenaering": [
                False,
                True,
            ],
            "er_steinerskole": [
                False,
                False,
            ],
            "er_norskskoleiutlandet": [
                False,
                False,
            ],
            "er_ukjentorgnrbed": [
                False,
                False,
            ],
            "er_over16aar": [
                False,
                False,
            ],
            "er_ikke_elevstatus_es": [
                False,
                False,
            ],
            "har_ikke_grunnskolepoeng": [
                False,
                False,
            ],
        }
    )

    result = _exclude_to_grunnskole_population(df)

    assert len(result) == 1


def test_exclude_to_grunnskole_population_ignore_grunnskolepoeng() -> None:
    df = pd.DataFrame(
        {
            "er_ikke_grunnskolenaering": [False],
            "er_steinerskole": [False],
            "er_norskskoleiutlandet": [False],
            "er_ukjentorgnrbed": [False],
            "er_over16aar": [False],
            "er_ikke_elevstatus_es": [False],
            "har_ikke_grunnskolepoeng": [True],
        }
    )

    result = _exclude_to_grunnskole_population(
        df,
        ignore_cols=[
            "har_ikke_grunnskolepoeng",
        ],
    )

    assert len(result) == 1


def test_exclude_to_grunnskole_population_returns_copy() -> None:
    df = pd.DataFrame(
        {
            "er_ikke_grunnskolenaering": [False],
            "er_steinerskole": [False],
            "er_norskskoleiutlandet": [False],
            "er_ukjentorgnrbed": [False],
            "er_over16aar": [False],
            "er_ikke_elevstatus_es": [False],
        }
    )

    result = _exclude_to_grunnskole_population(df)

    assert result is not df
