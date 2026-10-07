import pandas as pd
import pytest

from nudb_use.populations.grunnskole_population import (
    derive_har_grunnskolenaering,
    create_boolean_variables_for_exclusion,
    exclude_population,
)


def test_derive_har_grunnskolenaering_finds_codes():
    df = pd.DataFrame(
        {
            "bof_naering1_sn2025": ["85.201", "85.310", None],
            "bof_naering2_sn2025": [None, "85.202", None],
            "bof_naering3_sn2025": [None, None, None],
        }
    )

    result = derive_har_grunnskolenaering(df)

    assert result["har_grunnskolenaering"].tolist() == [
        True,
        True,
        False,
    ]


def test_derive_har_grunnskolenaering_custom_columns():
    df = pd.DataFrame(
        {
            "nace1_sn07": ["85.202", "85.310"],
            "nace2_sn07": [None, None],
            "nace3_sn07": [None, None],
        }
    )

    result = derive_har_grunnskolenaering(
        df,
        nace_columns=["nace1_sn07", "nace2_sn07", "nace3_sn07"],
    )

    assert result["har_grunnskolenaering"].tolist() == [
        True,
        False,
    ]


def test_create_boolean_variables_for_exclusion():
    df = pd.DataFrame(
        {
            "har_grunnskolenaering": [True, False],
            "gro_skolenavn_inn": ["Oslo skole", "Steinerskolen i Oslo"],
            "utd_skolekom": ["0301", "2599"],
            "orgnrbed": ["123", None],
            "pers_alder": [16, 17],
            "gro_elevstatus": ["E", "X"],
            "gr_grunnskolepoeng": [40.0, 0.0],
        }
    )

    result = create_boolean_variables_for_exclusion(df)

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


def test_elevstatus_not_used_when_all_values_are_missing():
    df = pd.DataFrame(
        {
            "har_grunnskolenaering": [True],
            "gro_skolenavn_inn": ["Oslo skole"],
            "utd_skolekom": ["0301"],
            "orgnrbed": ["123"],
            "pers_alder": [16],
            "gro_elevstatus": [None],
            "gr_grunnskolepoeng": [40.0],
        }
    )

    result = create_boolean_variables_for_exclusion(df)

    assert result["er_ikke_elevstatus_es"].eq(False).all()


def test_create_boolean_variables_raises_on_missing_columns():
    df = pd.DataFrame(
        {
            "har_grunnskolenaering": [True],
        }
    )

    with pytest.raises(ValueError):
        create_boolean_variables_for_exclusion(df)


def test_create_boolean_variables_res_false():
    df = pd.DataFrame(
        {
            "har_grunnskolenaering": [True],
            "gro_skolenavn_inn": ["Oslo skole"],
            "utd_skolekom": ["0301"],
            "orgnrbed": ["123"],
            "pers_alder": [16],
            "gro_elevstatus": ["E"],
        }
    )

    result = create_boolean_variables_for_exclusion(
        df,
        res=False,
    )

    assert "har_ikke_grunnskolepoeng" not in result.columns


def test_exclude_population():
    df = pd.DataFrame(
        {
            "er_ikke_grunnskolenaering": [False, True],
            "er_steinerskole": [False, False],
            "er_norskskoleiutlandet": [False, False],
            "er_ukjentorgnrbed": [False, False],
            "er_over16aar": [False, False],
            "er_ikke_elevstatus_es": [False, False],
            "har_ikke_grunnskolepoeng": [False, False],
            "snr": [1, 2],
        }
    )

    result = exclude_population(df)

    assert result["snr"].tolist() == [1]


def test_exclude_population_ignore_grunnskolepoeng():
    df = pd.DataFrame(
        {
            "er_ikke_grunnskolenaering": [False],
            "er_steinerskole": [False],
            "er_norskskoleiutlandet": [False],
            "er_ukjentorgnrbed": [False],
            "er_over16aar": [False],
            "er_ikke_elevstatus_es": [False],
            "har_ikke_grunnskolepoeng": [True],
            "snr": [1],
        }
    )

    result = exclude_population(
        df,
        ignore_cols=["har_ikke_grunnskolepoeng"],
    )

    assert len(result) == 1
