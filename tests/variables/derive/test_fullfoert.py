import datetime

import pandas as pd
import pytest

from nudb_use.variables.derive.fullfoert import gr_ergrunnskole_fullfoert
from nudb_use.variables.derive.fullfoert import uh_erbachelor_fullfoert
from nudb_use.variables.derive.fullfoert import uh_erdoktorgrad_fullfoert
from nudb_use.variables.derive.fullfoert import uh_erhoeyskolekandidat_fullfoert
from nudb_use.variables.derive.fullfoert import uh_ermaster_fullfoert
from nudb_use.variables.derive.fullfoert import vg_erstudiespess_fullfoert
from nudb_use.variables.derive.fullfoert import vg_ervgo_fullfoert
from nudb_use.variables.derive.fullfoert import vg_eryrkesfag_fullfoert
from nudb_use.variables.derive.registrert import PRG_RANGES


def test_gr_ergrunnskole_fullfoert() -> None:
    df = pd.DataFrame(
        {
            "nus2000": ["2000", "2000", "3000", "2000"],
            "uh_erutland": [False, True, False, False],
            "utd_fullfoertkode": ["8", "8", "8", "9"],
        }
    )

    result = gr_ergrunnskole_fullfoert(df)

    assert result["gr_ergrunnskole_fullfoert"].tolist() == [True, False, False, False]
    assert str(result["gr_ergrunnskole_fullfoert"].dtype) == "bool[pyarrow]"


def test_gr_ergrunnskole_fullfoert_priority_old() -> None:
    df = pd.DataFrame(
        {
            "nus2000": ["2000", "2000"],
            "uh_erutland": [False, False],
            "utd_fullfoertkode": ["9", "8"],
            "gr_ergrunnskole_fullfoert": [True, pd.NA],
        }
    )

    result = gr_ergrunnskole_fullfoert(df)

    assert result["gr_ergrunnskole_fullfoert"].tolist() == [True, True]


def test_vg_ervgo_fullfoert() -> None:
    df = pd.DataFrame(
        {
            "nus2000": ["4000", "5000", "5000", "3000"],
            "utd_fullfoertkode": ["8", "8", "9", "8"],
            "vg_kompetanse_nus": ["1", "9", "1", "1"],
            "utd_aktivitet_start": [
                datetime.datetime(2001, 1, 1),
                datetime.datetime(1999, 1, 1),
                datetime.datetime(2001, 1, 1),
                datetime.datetime(2001, 1, 1),
            ],
        }
    )

    result = vg_ervgo_fullfoert(df)

    assert result["vg_ervgo_fullfoert"].tolist() == [True, True, False, False]


def test_vg_erstudiespess_fullfoert_and_vg_eryrkesfag_fullfoert() -> None:
    df = pd.DataFrame(
        {
            "nus2000": ["4000", "5000", "4000"],
            "utd_fullfoertkode": ["8", "8", "9"],
            "vg_kompetanse_nus": ["1", "2", "1"],
            "utd_aktivitet_start": [
                datetime.datetime(2001, 1, 1),
                datetime.datetime(2001, 1, 1),
                datetime.datetime(2001, 1, 1),
            ],
            "vg_utdprogram": [
                PRG_RANGES["studiespess"][0],
                PRG_RANGES["yrkesfag"][0],
                PRG_RANGES["studiespess"][0],
            ],
        }
    )

    studiespess = vg_erstudiespess_fullfoert(df)
    yrkesfag = vg_eryrkesfag_fullfoert(df)

    assert studiespess["vg_erstudiespess_fullfoert"].tolist() == [True, False, False]
    assert yrkesfag["vg_eryrkesfag_fullfoert"].tolist() == [False, True, False]


def test_vg_fullfoert_logs_warning_on_invalid_program(
    caplog: pytest.LogCaptureFixture,
) -> None:
    df = pd.DataFrame(
        {
            "nus2000": ["40099"],
            "utd_fullfoertkode": ["8"],
            "vg_kompetanse_nus": ["1"],
            "utd_aktivitet_start": [datetime.datetime(2001, 1, 1)],
            "vg_utdprogram": ["101"],
        }
    )
    # Because of the try in the decorator, this only causes a logged warning
    vg_erstudiespess_fullfoert(df)
    assert "values outside valid codelist" in caplog.text
    assert "WARNING" in [rec.levelname for rec in caplog.records]


def test_uh_erhoeyskolekandidat_fullfoert() -> None:
    df = pd.DataFrame(
        {
            "nus2000": ["60099", "60099", "60099"],
            "utd_fullfoertkode": ["8", "8", "8"],
            "utd_klassetrinn": [
                15,
                15,
                17,
            ],  # Last should be false because of kltrinn != 15, 16
            "uh_gruppering_nus": ["03", "01", "03"],  # 01 should not count
        }
    )

    result = uh_erhoeyskolekandidat_fullfoert(df)

    assert result["uh_erhoeyskolekandidat_fullfoert"].tolist() == [
        True,
        False,
        False,
    ]


def test_uh_erbachelor_fullfoert() -> None:
    df = pd.DataFrame(
        {
            "nus2000": ["636102", "736104"],
            "utd_fullfoertkode": ["8", "8"],
        }
    )

    result = uh_erbachelor_fullfoert(df)

    assert result["uh_erbachelor_fullfoert"].tolist() == [True, False]


def test_uh_ermaster_and_uh_erdoktorgrad_fullfoert() -> None:
    df = pd.DataFrame(
        {
            "nus2000": ["7000", "8000", "7000"],
            "utd_fullfoertkode": ["8", "8", "9"],
        }
    )

    master = uh_ermaster_fullfoert(df)
    doktorgrad = uh_erdoktorgrad_fullfoert(df)

    assert master["uh_ermaster_fullfoert"].tolist() == [True, False, False]
    assert doktorgrad["uh_erdoktorgrad_fullfoert"].tolist() == [False, True, False]
