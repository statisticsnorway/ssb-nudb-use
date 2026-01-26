from pathlib import Path
from typing import Any

import nudb_config
import pandas as pd

import nudb_use
from nudb_use.variables.derive.registrert_foerste import first_registered_date_per_snr
from nudb_use.variables.derive.registrert_foerste import gr_foerste_registrert_dato
from nudb_use.variables.derive.registrert_foerste import uh_foerste_nus2000


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
                "10000",
                "40000",
                "60000",
                "70000",
                "60000",
                "80000",
                "50000",
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

    monkeypatch.setattr(nudb_use.paths.latest, "POSSIBLE_PATHS", [basepath])
    monkeypatch.setattr(nudb_config, "settings", new_settings)


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
    ]
    assert result["first_date"].tolist() == expected
    assert str(result["first_date"].dtype) == "datetime64[s]"


def test_uh_foerste_nus2000(tmp_path: Path, monkeypatch: Any) -> None:
    patch_wrap_join_helpers(tmp_path, monkeypatch)
    df = pd.DataFrame(
        {"snr": pd.Series(["a", "a", "b", "b", "c"], dtype="string[pyarrow]")}
    )

    result = uh_foerste_nus2000(df)

    values = result["uh_foerste_nus2000"].tolist()

    assert values[:4] == ["70000", "70000", "80000", "80000"]
    assert pd.isna(values[4])


def test_registrert_foerste_dato_derivations(tmp_path: Path, monkeypatch: Any) -> None:
    patch_wrap_join_helpers(tmp_path, monkeypatch)
    df = pd.DataFrame(
        {"snr": pd.Series(["a", "a", "b", "b", "c"], dtype="string[pyarrow]")}
    )

    gr = gr_foerste_registrert_dato(df)
    # vg = vg_foerste_registrert_dato(df)
    # uh = uh_foerste_registrert_dato(df)
    # bach = uh_bachelor_foerste_registrert_dato(df)
    # master = uh_master_foerste_registrert_dato(df)

    from nudb_use.nudb_logger import logger

    logger.critical(gr)
    assert gr["gr_foerste_registrert_dato"].tolist() == [
        pd.Timestamp("1970-01-01 00:01:40"),
        pd.Timestamp("1970-01-01 00:01:40"),
        pd.Timestamp("1970-01-01 00:00:30"),
        pd.Timestamp("1970-01-01 00:00:30"),
        pd.NaT,
    ]
    # assert vg["vg_foerste_registrert_dato"].tolist() == [
    #     pd.Timestamp("1970-01-01 00:03:20"),
    #     pd.Timestamp("1970-01-01 00:03:20"),
    #     pd.Timestamp("1970-01-01 00:02:30"),
    #     pd.Timestamp("1970-01-01 00:02:30"),
    #     pd.NaT,
    # ]
    # assert uh["uh_foerste_registrert_dato"].tolist() == [
    #     pd.NaT,
    #     pd.NaT,
    #     pd.Timestamp("1970-01-01 00:00:50"),
    #     pd.Timestamp("1970-01-01 00:00:50"),
    #     pd.NaT,
    # ]
    # assert bach["uh_bachelor_foerste_registrert_dato"].tolist() == [
    #     pd.Timestamp("1970-01-01 00:03:20"),
    #     pd.Timestamp("1970-01-01 00:03:20"),
    #     pd.NaT,
    #     pd.NaT,
    #     pd.NaT,
    # ]
    # assert master["uh_master_foerste_registrert_dato"].tolist() == [
    #     pd.NaT,
    #     pd.NaT,
    #     pd.Timestamp("1970-01-01 00:00:50"),
    #     pd.Timestamp("1970-01-01 00:00:50"),
    #     pd.NaT,
    # ]
