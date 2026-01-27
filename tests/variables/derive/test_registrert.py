import pandas as pd
import pytest

from nudb_use.nudb_logger import logger
from nudb_use.variables.derive.registrert import PRG_RANGES
from nudb_use.variables.derive.registrert import gr_ergrunnskole_registrering
from nudb_use.variables.derive.registrert import raise_vg_utdprogram_outside_ranges
from nudb_use.variables.derive.registrert import uh_erbachelor_registrering
from nudb_use.variables.derive.registrert import uh_erhoeyereutd_registrering
from nudb_use.variables.derive.registrert import uh_ermaster_registrering
from nudb_use.variables.derive.registrert import vg_erstudiespess_registrering
from nudb_use.variables.derive.registrert import vg_ervgo_registrering
from nudb_use.variables.derive.registrert import vg_eryrkesfag_registrering


def test_gr_ergrunnskole_registrering() -> None:
    df = pd.DataFrame(
        {"nus2000": ["2000", "2000", "3000"], "uh_erutland": [False, True, False]}
    )

    result = gr_ergrunnskole_registrering(df)

    assert result["gr_ergrunnskole_registrering"].tolist() == [True, False, False]
    assert str(result["gr_ergrunnskole_registrering"].dtype) == "bool[pyarrow]"


def test_gr_ergrunnskole_registrering_priority_old() -> None:
    df = pd.DataFrame(
        {
            "nus2000": ["2000", "2000"],
            "uh_erutland": [False, False],
            "gr_ergrunnskole_registrering": [False, pd.NA],
        }
    )

    result = gr_ergrunnskole_registrering(df)

    assert result["gr_ergrunnskole_registrering"].tolist() == [False, True]


def test_vg_ervgo_registrering() -> None:
    df = pd.DataFrame({"nus2000": ["3000", "4000", "5000"]})

    result = vg_ervgo_registrering(df)

    assert result["vg_ervgo_registrering"].tolist() == [True, True, False]


def test_vg_erstudiespess_registrering_and_vg_eryrkesfag_registrering() -> None:
    df = pd.DataFrame(
        {
            "nus2000": ["3000", "4000", "4000"],
            "vg_utdprogram": [
                PRG_RANGES["studiespess"][0],
                PRG_RANGES["yrkesfag"][0],
                PRG_RANGES["studiespess"][0],
            ],
        }
    )

    studiespess = vg_erstudiespess_registrering(df)
    yrkesfag = vg_eryrkesfag_registrering(df)

    assert studiespess["vg_erstudiespess_registrering"].tolist() == [
        True,
        False,
        True,
    ]
    assert yrkesfag["vg_eryrkesfag_registrering"].tolist() == [
        False,
        True,
        False,
    ]


def test_raise_vg_utdprogram_outside_ranges() -> None:
    ok = pd.Series([PRG_RANGES["studiespess"][0], PRG_RANGES["yrkesfag"][0]])
    raise_vg_utdprogram_outside_ranges(ok)

    with pytest.raises(ValueError):
        raise_vg_utdprogram_outside_ranges(pd.Series(["90"]))


def test_uh_erhoeyereutd_registrering() -> None:
    df = pd.DataFrame({"nus2000": ["6000", "7000", "8000", "5000"]})

    result = uh_erhoeyereutd_registrering(df)

    assert result["uh_erhoeyereutd_registrering"].tolist() == [
        True,
        True,
        True,
        False,
    ]


def test_uh_erbachelor_registrering() -> None:
    df = pd.DataFrame(
        {"nus2000": ["60000", "610000"], "uh_gruppering_nus": ["000B", "000M"]}
    )

    result = uh_erbachelor_registrering(df)
    logger.info(df.columns)
    logger.info(df)
    assert result["uh_erbachelor_registrering"].tolist() == [True, False]


def test_uh_ermaster_registrering() -> None:
    df = pd.DataFrame({"nus2000": ["7000", "8000"]})

    result = uh_ermaster_registrering(df)

    assert result["uh_ermaster_registrering"].tolist() == [True, False]
