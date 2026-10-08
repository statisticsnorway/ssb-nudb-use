from pathlib import Path
from typing import Any

import pandas as pd

import nudb_use
from nudb_use.datasets import reset_nudb_database
from nudb_use.variables.specific_vars.utd_hendelse_id import (
    join_variables_on_hendelse_id,
)


def patch_wrap_hendelse_id_helpers(tmp_path: Path, monkeypatch: Any) -> None:
    reset_nudb_database()

    basepath = tmp_path / "local" / "nudb-data"
    nudbpath = basepath / "klargjorte-data"
    nudbpath.mkdir(parents=True)

    avslutta = pd.DataFrame(
        {
            "utd_hendelse_id": [
                0,  # 00000000000000001,
                1,  # 00000000000000002,
                2,  # 00000000000000003,
            ],
            "snr": ["a", "b", "c"],
            "nus2000": [
                "210301",
                "310021",
                "405001",
            ],
            "utd_fullfoertkode": [
                "2",
                "8",
                "8",
            ],
        }
    ).astype(
        {
            "utd_hendelse_id": "UInt64",
            "snr": "string[pyarrow]",
            "nus2000": "string[pyarrow]",
            "utd_fullfoertkode": "string[pyarrow]",
        }
    )

    igang = pd.DataFrame(
        {
            "utd_hendelse_id": [
                10000000000000000,
                10000000000000001,
                10000000000000002,
            ],
            "snr": ["a", "b", "c"],
            "nus2000": [
                "201719",
                "320239",
                "440119",
            ],
            "utd_skolekom": [
                "2580",
                "0301",
                "0301",
            ],
        }
    ).astype(
        {
            "utd_hendelse_id": "UInt64",
            "snr": "string[pyarrow]",
            "nus2000": "string[pyarrow]",
            "utd_skolekom": "string[pyarrow]",
        }
    )

    eksamen = pd.DataFrame(
        {
            "utd_hendelse_id": [
                20000000000000000,
                20000000000000001,
                20000000000000002,
            ],
            "snr": ["a", "b", "c"],
            "nus2000": [
                "210219",
                "313020",
                "425010",
            ],
            "uh_eksamen_karakter": [
                "F",
                "A",
                "B",
            ],
        }
    ).astype(
        {
            "utd_hendelse_id": "UInt64",
            "snr": "string[pyarrow]",
            "nus2000": "string[pyarrow]",
            "uh_eksamen_karakter": "string[pyarrow]",
        }
    )

    igang.to_parquet(nudbpath / "igang_p1970_p1971_v1.parquet")
    avslutta.to_parquet(nudbpath / "avslutta_p1970_p1971_v1.parquet")
    eksamen.to_parquet(nudbpath / "eksamen_p1970_p1971_v1.parquet")

    # legg inn i config at alle registreringer trenger flere (potensielt) dato-kolonner
    monkeypatch.setattr(nudb_use.paths.latest, "POSSIBLE_PATHS", [basepath])


def test_join_variables_on_hendelse_id(tmp_path: Path, monkeypatch: Any) -> None:
    patch_wrap_hendelse_id_helpers(tmp_path, monkeypatch)

    df = pd.DataFrame(
        {
            "utd_hendelse_id": [
                0,
                1,
                2,
                10000000000000000,
                10000000000000001,
                10000000000000002,
                20000000000000000,
                20000000000000001,
                20000000000000002,
            ]
        }
    )

    df2 = join_variables_on_hendelse_id(
        df,
        variables=[
            "snr",
            "nus2000",
            "uh_eksamen_karakter",
            "utd_skolekom",
            "utd_fullfoertkode",
        ],
    ).fillna("999")

    assert df2["snr"].to_list() == ["a", "b", "c", "a", "b", "c", "a", "b", "c"]

    assert df2["nus2000"].to_list() == [
        "210301",
        "310021",
        "405001",
        "201719",
        "320239",
        "440119",
        "210219",
        "313020",
        "425010",
    ]

    assert df2["uh_eksamen_karakter"].to_list() == [
        "999",
        "999",
        "999",
        "999",
        "999",
        "999",
        "F",
        "A",
        "B",
    ]
