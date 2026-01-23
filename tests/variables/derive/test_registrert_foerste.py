from types import SimpleNamespace
from typing import Any

import pandas as pd

from nudb_use.variables.derive import derive_decorator
from nudb_use.variables.derive.registrert_foerste import first_registered_date_per_snr
from nudb_use.variables.derive.registrert_foerste import gr_foerste_registrert_dato
from nudb_use.variables.derive.registrert_foerste import (
    uh_bachelor_foerste_registrert_dato,
)
from nudb_use.variables.derive.registrert_foerste import uh_foerste_nus2000
from nudb_use.variables.derive.registrert_foerste import uh_foerste_registrert_dato
from nudb_use.variables.derive.registrert_foerste import (
    uh_master_foerste_registrert_dato,
)
from nudb_use.variables.derive.registrert_foerste import vg_foerste_registrert_dato


def patch_wrap_join_helpers(monkeypatch: Any) -> None:
    monkeypatch.setattr(derive_decorator, "get_source_data", lambda _name, df: df)
    monkeypatch.setattr(
        derive_decorator, "join_variable_data", lambda _name, derived, _df: derived
    )
    monkeypatch.setattr(
        derive_decorator,
        "settings",
        SimpleNamespace(
            variables={
                "uh_foerste_nus2000": SimpleNamespace(
                    derived_from=["snr", "nus2000", "utd_aktivitet_start"]
                ),
                "gr_foerste_registrert_dato": SimpleNamespace(
                    derived_from=[
                        "snr",
                        "gr_ergrunnskole_registrering",
                        "utd_aktivitet_start",
                    ]
                ),
                "vg_foerste_registrert_dato": SimpleNamespace(
                    derived_from=[
                        "snr",
                        "vg_ervgo_registrering",
                        "utd_aktivitet_start",
                    ]
                ),
                "uh_foerste_registrert_dato": SimpleNamespace(
                    derived_from=[
                        "snr",
                        "uh_erhoyereutd_registrering",
                        "utd_aktivitet_start",
                    ]
                ),
                "uh_bachelor_foerste_registrert_dato": SimpleNamespace(
                    derived_from=[
                        "snr",
                        "uh_erbachelor_registrering",
                        "utd_aktivitet_start",
                    ]
                ),
                "uh_master_foerste_registrert_dato": SimpleNamespace(
                    derived_from=[
                        "snr",
                        "uh_ermaster_registrering",
                        "utd_aktivitet_start",
                    ]
                ),
            }
        ),
    )


def test_first_registered_date_per_snr() -> None:
    df = pd.DataFrame(
        {
            "snr": ["a", "a", "b", "b", "c"],
            "utd_aktivitet_start": [100, 200, 150, 50, 300],
            "flag": [True, False, False, True, pd.NA],
        }
    )

    result = first_registered_date_per_snr(
        df, variable_name="first_date", filter_var="flag"
    )

    expected = [
        pd.Timestamp("1970-01-01 00:01:40"),
        pd.Timestamp("1970-01-01 00:00:50"),
        pd.NaT,
    ]
    assert result["first_date"].tolist() == expected
    assert str(result.dtype) == "datetime64[s]"


def test_uh_foerste_nus2000(monkeypatch: Any) -> None:
    patch_wrap_join_helpers(monkeypatch)
    df = pd.DataFrame(
        {
            "snr": ["a", "a", "b", "b", "c"],
            "nus2000": ["6000", "5000", "7000", "8000", "4000"],
            "utd_aktivitet_start": [200, 100, 300, 100, 50],
        }
    )

    result = uh_foerste_nus2000(df)

    values = result["uh_foerste_nus2000"].tolist()
    assert values[:2] == ["6000", "8000"]
    assert pd.isna(values[2])


def test_registrert_foerste_dato_derivations(monkeypatch: Any) -> None:
    patch_wrap_join_helpers(monkeypatch)
    df = pd.DataFrame(
        {
            "snr": ["a", "a", "b", "b", "c"],
            "utd_aktivitet_start": [100, 200, 150, 50, 300],
            "gr_ergrunnskole_registrering": [True, False, False, True, False],
            "vg_ervgo_registrering": [False, True, True, False, False],
            "uh_erhoyereutd_registrering": [False, False, True, True, False],
            "uh_erbachelor_registrering": [False, True, False, False, False],
            "uh_ermaster_registrering": [False, False, False, True, False],
        }
    )

    gr = gr_foerste_registrert_dato(df)
    vg = vg_foerste_registrert_dato(df)
    uh = uh_foerste_registrert_dato(df)
    bach = uh_bachelor_foerste_registrert_dato(df)
    master = uh_master_foerste_registrert_dato(df)

    assert gr["gr_foerste_registrert_dato"].tolist() == [
        pd.Timestamp("1970-01-01 00:01:40"),
        pd.Timestamp("1970-01-01 00:01:40"),
        pd.Timestamp("1970-01-01 00:00:50"),
        pd.Timestamp("1970-01-01 00:00:50"),
        pd.NaT,
    ]
    assert vg["vg_foerste_registrert_dato"].tolist() == [
        pd.Timestamp("1970-01-01 00:03:20"),
        pd.Timestamp("1970-01-01 00:03:20"),
        pd.Timestamp("1970-01-01 00:02:30"),
        pd.Timestamp("1970-01-01 00:02:30"),
        pd.NaT,
    ]
    assert uh["uh_foerste_registrert_dato"].tolist() == [
        pd.NaT,
        pd.NaT,
        pd.Timestamp("1970-01-01 00:00:50"),
        pd.Timestamp("1970-01-01 00:00:50"),
        pd.NaT,
    ]
    assert bach["uh_bachelor_foerste_registrert_dato"].tolist() == [
        pd.Timestamp("1970-01-01 00:03:20"),
        pd.Timestamp("1970-01-01 00:03:20"),
        pd.NaT,
        pd.NaT,
        pd.NaT,
    ]
    assert master["uh_master_foerste_registrert_dato"].tolist() == [
        pd.NaT,
        pd.NaT,
        pd.Timestamp("1970-01-01 00:00:50"),
        pd.Timestamp("1970-01-01 00:00:50"),
        pd.NaT,
    ]
