from pathlib import Path
from typing import Any

import pandas as pd

import nudb_use
from nudb_use.datasets import NudbData
from nudb_use.datasets import reset_nudb_database
from nudb_use.metadata.nudb_config.variable_names import update_colnames


def patch_nudb_database(
    igang: pd.DataFrame,
    avslutta: pd.DataFrame,
    eksamen: pd.DataFrame,
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    reset_nudb_database()

    basepath = tmp_path / "local" / "nudb-data"
    nudbpath = basepath / "klargjorte-data"
    nudbpath.mkdir(parents=True)

    igang = update_colnames(igang)
    avslutta = update_colnames(avslutta)
    eksamen = update_colnames(eksamen)

    eksamen["uh_eksamen_er_gjentak"] = eksamen["uh_eksamen_er_gjentak"] == "d"

    igang.to_parquet(nudbpath / "igang_p1970_p1971_v1.parquet")
    avslutta.to_parquet(nudbpath / "avslutta_p1970_p1971_v1.parquet")
    eksamen.to_parquet(nudbpath / "eksamen_p1970_p1971_v1.parquet")

    # legg inn i config at alle registreringer trenger flere (potensielt) dato-kolonner
    monkeypatch.setattr(nudb_use.paths.latest, "POSSIBLE_PATHS", [basepath])


def test_nudbdata(
    igang: pd.DataFrame,
    avslutta: pd.DataFrame,
    eksamen: pd.DataFrame,
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    patch_nudb_database(igang, avslutta, eksamen, tmp_path, monkeypatch)

    NudbData("utd_hoeyeste")
    NudbData("igang")
