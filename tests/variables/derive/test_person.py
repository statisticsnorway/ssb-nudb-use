from pathlib import Path
from typing import Any

import pandas as pd

import nudb_use
from nudb_use.datasets import reset_nudb_database
from nudb_use.variables import derive


def patch_nudb_database(
    freg_situttak: pd.DataFrame,
    innvbef: pd.DataFrame,
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    reset_nudb_database()

    shared_root = tmp_path / "shared"
    bef_statistikk = shared_root / "bef-statistikk"
    innvbef_dir = bef_statistikk / "folketall" / "innvbef" / "2024"
    freg_dir = bef_statistikk / "freg-situttak" / "2026"

    innvbef_dir.mkdir(parents=True)
    freg_dir.mkdir(parents=True)

    freg_situttak.to_parquet(freg_dir / "freg_situasjonsuttak_p2026-01-31_v1.parquet")
    innvbef.to_parquet(innvbef_dir / "innvbef_p2024-12-31_v1.parquet")

    monkeypatch.setattr(nudb_use.paths.latest, "SHARED_ROOT", shared_root)


def test_pers_variables(
    igang: pd.DataFrame,
    freg_situttak: pd.DataFrame,
    innvbef: pd.DataFrame,
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    patch_nudb_database(freg_situttak, innvbef, tmp_path, monkeypatch)

    _tmp = (
        igang.copy()
        .pipe(derive.pers_invkat)
        .pipe(derive.pers_kjoenn)
        .pipe(derive.pers_foedselsdato)
    )
