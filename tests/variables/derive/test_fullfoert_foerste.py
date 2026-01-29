from pathlib import Path
from typing import Any

import pandas as pd

import nudb_use
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

    avslutta = pd.DataFrame(
        {
            "snr": [
                "a",
                "a",
                "b",
                "b",
                "c",
                "c",
                "b",
                "c",
            ],
            "utd_aktivitet_slutt": [
                pd.Timestamp("1970-01-01 00:00:10"),
                pd.Timestamp("1970-01-01 00:00:15"),
                pd.Timestamp("1971-01-01 00:07:40"),
                pd.Timestamp("1970-01-01 00:00:20"),
                pd.Timestamp("1971-01-01 00:01:30"),
                pd.Timestamp("1971-01-01 00:02:40"),
                pd.Timestamp("1971-01-01 00:01:30"),
                pd.Timestamp("1971-01-01 00:05:00"),
            ],
            "utd_aktivitet_start": [
                pd.Timestamp("1970-01-01 00:00:09"),
                pd.Timestamp("1970-01-01 00:00:14"),
                pd.Timestamp("1971-01-01 00:07:39"),
                pd.Timestamp("1970-01-01 00:00:19"),
                pd.Timestamp("1971-01-01 00:01:29"),
                pd.Timestamp("1971-01-01 00:02:39"),
                pd.Timestamp("1971-01-01 00:01:29"),
                pd.Timestamp("1971-01-01 00:04:00"),
            ],
            "nus2000": [
                "20001",
                "416402",
                "20001",
                "753106",
                "401104",
                "623303",
                "636102",
                "836105",
            ],
            "utd_fullfoertkode": [
                "1",
                "8",
                "8",
                "8",
                "8",
                "8",
                "8",
                "8",
            ],
            "utd_erutland": [
                True,
                False,
                False,
                False,
                False,
                False,
                False,
                False,
            ],
            "vg_utdprogram": [
                "01",
                "01",
                "21",
                "70",
                "30",
                pd.NA,
                pd.NA,
                pd.NA,
            ],
            "utd_klassetrinn": [
                pd.NA,
                pd.NA,
                pd.NA,
                pd.NA,
                pd.NA,
                "15",
                "16",
                "20",
            ],
        }
    ).astype(
        {
            "snr": "string[pyarrow]",
            "vg_utdprogram": "string[pyarrow]",
            "utd_aktivitet_slutt": "datetime64[s]",
            "nus2000": "string[pyarrow]",
            "utd_fullfoertkode": "string[pyarrow]",
            "utd_erutland": "bool[pyarrow]",
        }
    )

    igang = pd.DataFrame({"snr": ["a"]})
    eksamen = pd.DataFrame({"snr": ["a"]})

    igang.to_parquet(nudbpath / "igang_p1970_p1971_v1.parquet")
    avslutta.to_parquet(nudbpath / "avslutta_p1970_p1971_v1.parquet")
    eksamen.to_parquet(nudbpath / "eksamen_p1970_p1971_v1.parquet")

    # legg inn i config at alle registreringer trenger flere (potensielt) dato-kolonner
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


def test_fullfoert_foerste_dato_derivations(tmp_path: Path, monkeypatch: Any) -> None:
    patch_wrap_join_helpers(tmp_path, monkeypatch)
    df = pd.DataFrame(
        {
            "snr": ["a", "a", "b", "b", "c"],
            "utd_aktivitet_slutt": [100, 200, 150, 50, 300],
        }
    )

    pd.set_option("display.max_columns", None)
    gr = gr_foerste_fullfoert_dato(df)
    vg = vg_foerste_fullfoert_dato(df)
    studiespess = vg_studiespess_foerste_fullfoert_dato(df)
    yrkesfag = vg_yrkesfag_foerste_fullfoert_dato(df)
    hoey = uh_hoeyskolekandidat_foerste_fullfoert_dato(df)
    bach = uh_bachelor_foerste_fullfoert_dato(df)
    master = uh_master_foerste_fullfoert_dato(df)
    doktor = uh_doktorgrad_foerste_fullfoert_dato(df)

    assert "utd_aktivitet_slutt_x" not in vg.columns

    from nudb_use.nudb_logger import logger

    logger.critical(doktor)

    assert gr["gr_foerste_fullfoert_dato"].tolist() == [
        pd.NaT,
        pd.NaT,
        pd.Timestamp("1971-01-01 00:07:40"),
        pd.Timestamp("1971-01-01 00:07:40"),
        pd.NaT,
    ]
    assert vg["vg_foerste_fullfoert_dato"].tolist() == [
        pd.Timestamp("1970-01-01 00:00:15"),
        pd.Timestamp("1970-01-01 00:00:15"),
        pd.NaT,
        pd.NaT,
        pd.Timestamp("1971-01-01 00:01:30"),
    ]
    assert studiespess["vg_studiespess_foerste_fullfoert_dato"].tolist() == [
        pd.Timestamp("1970-01-01 00:00:15"),
        pd.Timestamp("1970-01-01 00:00:15"),
        pd.NaT,
        pd.NaT,
        pd.NaT,
    ]
    assert yrkesfag["vg_yrkesfag_foerste_fullfoert_dato"].tolist() == [
        pd.NaT,
        pd.NaT,
        pd.NaT,
        pd.NaT,
        pd.Timestamp("1971-01-01 00:01:30"),
    ]
    assert hoey["uh_hoeyskolekandidat_foerste_fullfoert_dato"].tolist() == [
        pd.NaT,
        pd.NaT,
        pd.Timestamp("1971-01-01 00:01:30"),
        pd.Timestamp("1971-01-01 00:01:30"),
        pd.Timestamp("1971-01-01 00:02:40"),
    ]
    assert bach["uh_bachelor_foerste_fullfoert_dato"].tolist() == [
        pd.NaT,
        pd.NaT,
        pd.Timestamp("1971-01-01 00:01:30"),
        pd.Timestamp("1971-01-01 00:01:30"),
        pd.NaT,
    ]
    assert master["uh_master_foerste_fullfoert_dato"].tolist() == [
        pd.NaT,
        pd.NaT,
        pd.Timestamp("1970-01-01 00:00:20"),
        pd.Timestamp("1970-01-01 00:00:20"),
        pd.NaT,
    ]
    assert doktor["uh_doktorgrad_foerste_fullfoert_dato"].tolist() == [
        pd.NaT,
        pd.NaT,
        pd.NaT,
        pd.NaT,
        pd.Timestamp("1971-01-01 00:05:00"),
    ]
