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
    uh_hoeyskolekandidat_foerste_fullfoert_dato,
)
from nudb_use.variables.derive.fullfoert_foerste import uh_master_foerste_fullfoert_dato
from nudb_use.variables.derive.fullfoert_foerste import vg_foerste_fullfoert_dato
from nudb_use.variables.derive.fullfoert_foerste import (
    vg_studiespess_foerste_fullfoert_dato,
)
from nudb_use.variables.derive.fullfoert_foerste import (
    vg_yrkesfag_foerste_fullfoert_dato,
)


def patch_wrap_join_helpers(tmp_path: Path, monkeypatch: Any) -> None:

    basepath = tmp_path / "local" / "nudb-data"
    nudbpath = basepath / "klargjorte-data"
    nudbpath.mkdir(parents=True)

    igang = pd.DataFrame(
        {
            "snr": ["a", "a", "b", "b", "c", "a", "a", "b", "b", "c"],
            "utd_aktivitet_start": [
                pd.Timestamp("1970-01-01 00:01:40"),
                pd.Timestamp("1970-01-01 00:03:40"),
                pd.Timestamp("1970-01-01 00:07:40"),
                pd.Timestamp("1970-01-01 00:00:30"),
                pd.Timestamp("1970-01-01 00:01:30"),
                pd.Timestamp("1970-01-01 00:01:40"),
                pd.Timestamp("1970-01-01 00:00:40"),
                pd.Timestamp("1970-01-01 00:07:40"),
                pd.Timestamp("1970-01-01 00:00:30"),
                pd.Timestamp("1970-01-01 00:01:30"),
            ],
            "nus2000": [
                "20000",
                "20000",
                "20000",
                "40000",
                "40000",
                "636102",
                "753106",
                "636102",
                "80000",
                "40000",
            ],
            "uh_erutland": [
                True,
                False,
                False,
                False,
                False,
                False,
                False,
                False,
                False,
                False,
            ],
        }
    ).astype(
        {
            "snr": "string[pyarrow]",
            "utd_aktivitet_start": "datetime64[s]",
            "nus2000": "string[pyarrow]",
            "uh_erutland": "bool[pyarrow]",
        }
    )

    avslutta = pd.DataFrame(
        {
            "snr": ["a", "a", "b", "b", "c"],
            "utd_aktivitet_slutt": [
                pd.Timestamp("1971-01-01 00:01:40"),
                pd.Timestamp("1971-01-01 00:03:40"),
                pd.Timestamp("1971-01-01 00:07:40"),
                pd.Timestamp("1971-01-01 00:00:30"),
                pd.Timestamp("1971-01-01 00:01:30"),
            ],
            "nus2000": ["20001", "20001", "20001", "10001", "40001"],
            "uh_erutland": [True, False, False, False, False],
        }
    ).astype(
        {
            "snr": "string[pyarrow]",
            "utd_aktivitet_slutt": "datetime64[s]",
            "nus2000": "string[pyarrow]",
            "uh_erutland": "bool[pyarrow]",
        }
    )

    eksamen = pd.DataFrame(
        {
            "snr": pd.Series(["a", "a", "b", "b", "c"], dtype="string[pyarrow]"),
            "utd_eksamen_dato": [
                pd.Timestamp("1971-01-01 00:01:45"),
                pd.Timestamp("1971-01-01 00:03:45"),
                pd.Timestamp("1971-01-01 00:07:45"),
                pd.Timestamp("1971-01-01 00:00:35"),
                pd.Timestamp("1971-01-01 00:01:35"),
            ],
            "nus2000": ["20002", "20002", "20002", "10002", "40002"],
            "uh_erutland": [True, False, False, False, False],
        }
    ).astype(
        {
            "snr": "string[pyarrow]",
            "utd_eksamen_dato": "datetime64[s]",
            "nus2000": "string[pyarrow]",
            "uh_erutland": "bool[pyarrow]",
        }
    )

    igang.to_parquet(nudbpath / "igang_p1970_p1971_v1.parquet")
    avslutta.to_parquet(nudbpath / "avslutta_p1970_p1971_v1.parquet")
    eksamen.to_parquet(nudbpath / "eksamen_p1970_p1971_v1.parquet")

    new_settings = nudb_config.settings.model_copy()
    new_settings.variables.uh_foerste_nus2000.derived_uses_datasets = [
        "igang",
        "avslutta",
        "eksamen",
    ]
    new_settings.variables.uh_foerste_nus2000.derived_join_keys = ["snr"]
    new_settings.variables.uh_erbachelor_registrering.derived_from = [
        "nus2000",
        "uh_gradmerke_nus",
    ]
    new_settings.variables.uh_erbachelor_fullfoert.derived_from = [
        "utd_fullfortkode",
        "uh_gradmerke_nus",
    ]

    monkeypatch.setattr(nudb_use.paths.latest, "POSSIBLE_PATHS", [basepath])



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
            "gr_ergrunnskole_fullfoert": [True, False, False, True, False],
            "vg_ervgo_fullfoert": [False, True, True, False, False],
            "vg_erstudiespess_fullfoert": [True, False, False, False, False],
            "vg_eryrkesfag_fullfoert": [False, False, True, False, False],
            "uh_erhoeyskolekandidat_fullfoert": [False, True, False, False, False],
            "uh_erbachelor_fullfoert": [False, False, True, False, False],
            "uh_ermaster_fullfoert": [False, False, False, True, False],
            "uh_erdoktorgrad_fullfoert": [False, False, False, False, True],
        }
    )

    gr = gr_foerste_fullfoert_dato(df)
    vg = vg_foerste_fullfoert_dato(df)
    studiespess = vg_studiespess_foerste_fullfoert_dato(df)
    yrkesfag = vg_yrkesfag_foerste_fullfoert_dato(df)
    hoey = uh_hoeyskolekandidat_foerste_fullfoert_dato(df)
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
    assert hoey["uh_hoeyskolekandidat_foerste_fullfoert_dato"].tolist() == [
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
