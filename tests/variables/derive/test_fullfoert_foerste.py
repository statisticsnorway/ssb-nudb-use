from types import SimpleNamespace
from typing import Any

import pandas as pd

from nudb_use.variables.derive import derive_decorator
from nudb_use.variables.derive.fullfoert_foerste import first_end_date_per_snr
from nudb_use.variables.derive.fullfoert_foerste import gr_foerste_fullfoert_dato
from nudb_use.variables.derive.fullfoert_foerste import (
    uh_bachelor_foerste_fullfoert_dato,
)
from nudb_use.variables.derive.fullfoert_foerste import (
    uh_doktorgrad_foerste_fullfoert_dato,
)
from nudb_use.variables.derive.fullfoert_foerste import (
    uh_hoyskolekandidat_foerste_fullfoert_dato,
)
from nudb_use.variables.derive.fullfoert_foerste import uh_master_foerste_fullfoert_dato
from nudb_use.variables.derive.fullfoert_foerste import vg_foerste_fullfoert_dato
from nudb_use.variables.derive.fullfoert_foerste import (
    vg_studiespess_foerste_fullfoert_dato,
)
from nudb_use.variables.derive.fullfoert_foerste import (
    vg_yrkesfag_foerste_fullfoert_dato,
)


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
                "gr_foerste_fullfoert_dato": SimpleNamespace(
                    derived_from=[
                        "snr",
                        "gr_ergrunnskole_fullfort",
                        "utd_aktivitet_slutt",
                    ]
                ),
                "vg_foerste_fullfoert_dato": SimpleNamespace(
                    derived_from=[
                        "snr",
                        "vg_ervgo_fullfort",
                        "utd_aktivitet_slutt",
                    ]
                ),
                "vg_studiespess_foerste_fullfoert_dato": SimpleNamespace(
                    derived_from=[
                        "snr",
                        "vg_erstudiespess_fullfort",
                        "utd_aktivitet_slutt",
                    ]
                ),
                "vg_yrkesfag_foerste_fullfoert_dato": SimpleNamespace(
                    derived_from=[
                        "snr",
                        "vg_eryrkesfag_fullfort",
                        "utd_aktivitet_slutt",
                    ]
                ),
                "uh_hoyskolekandidat_foerste_fullfoert_dato": SimpleNamespace(
                    derived_from=[
                        "snr",
                        "uh_erhoyskolekandidat_fullfort",
                        "utd_aktivitet_slutt",
                    ]
                ),
                "uh_bachelor_foerste_fullfoert_dato": SimpleNamespace(
                    derived_from=[
                        "snr",
                        "uh_erbachelor_fullfort",
                        "utd_aktivitet_slutt",
                    ]
                ),
                "uh_master_foerste_fullfoert_dato": SimpleNamespace(
                    derived_from=[
                        "snr",
                        "uh_ermaster_fullfort",
                        "utd_aktivitet_slutt",
                    ]
                ),
                "uh_doktorgrad_foerste_fullfoert_dato": SimpleNamespace(
                    derived_from=[
                        "snr",
                        "uh_erdoktorgrad_fullfort",
                        "utd_aktivitet_slutt",
                    ]
                ),
            }
        ),
    )


def test_first_end_date_per_snr() -> None:
    df = pd.DataFrame(
        {
            "snr": ["a", "a", "b", "b", "c"],
            "utd_aktivitet_slutt": [100, 200, 150, 50, 300],
            "flag": [True, False, False, True, pd.NA],
        }
    )

    result = first_end_date_per_snr(df, variable_name="first_end", filter_var="flag")

    expected = [
        pd.Timestamp("1970-01-01 00:01:40"),
        pd.Timestamp("1970-01-01 00:00:50"),
    ]

    assert result["first_end"].tolist() == expected
    assert str(result["first_end"].dtype) == "datetime64[s]"


def test_fullfoert_foerste_dato_derivations(monkeypatch: Any) -> None:
    patch_wrap_join_helpers(monkeypatch)
    df = pd.DataFrame(
        {
            "snr": ["a", "a", "b", "b", "c"],
            "utd_aktivitet_slutt": [100, 200, 150, 50, 300],
            "gr_ergrunnskole_fullfort": [True, False, False, True, False],
            "vg_ervgo_fullfort": [False, True, True, False, False],
            "vg_erstudiespess_fullfort": [True, False, False, False, False],
            "vg_eryrkesfag_fullfort": [False, False, True, False, False],
            "uh_erhoyskolekandidat_fullfort": [False, True, False, False, False],
            "uh_erbachelor_fullfort": [False, False, True, False, False],
            "uh_ermaster_fullfort": [False, False, False, True, False],
            "uh_erdoktorgrad_fullfort": [False, False, False, False, True],
        }
    )

    gr = gr_foerste_fullfoert_dato(df)
    vg = vg_foerste_fullfoert_dato(df)
    studiespess = vg_studiespess_foerste_fullfoert_dato(df)
    yrkesfag = vg_yrkesfag_foerste_fullfoert_dato(df)
    hoy = uh_hoyskolekandidat_foerste_fullfoert_dato(df)
    bach = uh_bachelor_foerste_fullfoert_dato(df)
    master = uh_master_foerste_fullfoert_dato(df)
    doktor = uh_doktorgrad_foerste_fullfoert_dato(df)

    print(df)
    assert gr["gr_foerste_fullfoert_dato"].tolist() == [
        pd.Timestamp("1970-01-01 00:01:40"),
        pd.Timestamp("1970-01-01 00:01:40"),
        pd.Timestamp("1970-01-01 00:00:50"),
        pd.Timestamp("1970-01-01 00:00:50"),
        pd.NaT,
    ]
    assert vg["vg_foerste_fullfoert_dato"].tolist() == [
        pd.Timestamp("1970-01-01 00:03:20"),
        pd.Timestamp("1970-01-01 00:03:20"),
        pd.Timestamp("1970-01-01 00:02:30"),
        pd.Timestamp("1970-01-01 00:02:30"),
        pd.NaT,
    ]
    assert studiespess["vg_studiespess_foerste_fullfoert_dato"].tolist() == [
        pd.Timestamp("1970-01-01 00:01:40"),
        pd.Timestamp("1970-01-01 00:01:40"),
        pd.NaT,
        pd.NaT,
        pd.NaT,
    ]
    assert yrkesfag["vg_yrkesfag_foerste_fullfoert_dato"].tolist() == [
        pd.NaT,
        pd.NaT,
        pd.Timestamp("1970-01-01 00:02:30"),
        pd.Timestamp("1970-01-01 00:02:30"),
        pd.NaT,
    ]
    assert hoy["uh_hoyskolekandidat_foerste_fullfoert_dato"].tolist() == [
        pd.Timestamp("1970-01-01 00:03:20"),
        pd.Timestamp("1970-01-01 00:03:20"),
        pd.NaT,
        pd.NaT,
        pd.NaT,
    ]
    assert bach["uh_bachelor_foerste_fullfoert_dato"].tolist() == [
        pd.NaT,
        pd.NaT,
        pd.Timestamp("1970-01-01 00:02:30"),
        pd.Timestamp("1970-01-01 00:02:30"),
        pd.NaT,
    ]
    assert master["uh_master_foerste_fullfoert_dato"].tolist() == [
        pd.NaT,
        pd.NaT,
        pd.Timestamp("1970-01-01 00:00:50"),  # This row is joined back onto snr "b"
        pd.Timestamp("1970-01-01 00:00:50"),
        pd.NaT,
    ]
    assert doktor["uh_doktorgrad_foerste_fullfoert_dato"].tolist() == [
        pd.NaT,
        pd.NaT,
        pd.NaT,
        pd.NaT,
        pd.Timestamp("1970-01-01 00:05:00"),
    ]
