from pathlib import Path
from typing import Any

from nudb_use.paths import latest


def test_find_file_custom_dir(monkeypatch: Any, tmp_path: Path) -> None:
    semipath = tmp_path / "local" / "nudb-data"
    fullpath = semipath / "klargjorte-data"
    fullpath.mkdir(parents=True)

    filepath = fullpath / "tmp.parquet"
    filepath.write_text("hello world!", encoding="utf-8")

    # Patch the global path list to only use the test directory
    monkeypatch.setattr(latest, "POSSIBLE_PATHS", [semipath])

    latest._add_delt_path(semipath)
    result = latest._get_available_files()[0]

    assert result.name == "tmp.parquet"


def test_latest_shared_paths_builds_mapping(monkeypatch: Any, tmp_path: Path) -> None:
    base = tmp_path / "mount"
    (base / "klargjorte-data").mkdir(parents=True)

    monkeypatch.setattr(latest, "POSSIBLE_PATHS", [])
    latest._add_delt_path(base)
    file_a = base / "klargjorte-data" / "a_p2021-01-01_v1.parquet"
    file_b = base / "klargjorte-data" / "b_p2021-02-02_v2.parquet"
    file_a.write_text("a")
    file_b.write_text("b")

    paths = latest.latest_shared_paths()

    assert paths == {"a": file_a, "b": file_b}
    assert latest.latest_shared_paths("a") == file_a


def test_hide_reveal_produkt_path() -> None:
    produkt = Path(latest.PRODUKT)
    assert produkt in latest.POSSIBLE_PATHS

    latest._hide_produkt_paths()  # run twice to check both branches
    latest._hide_produkt_paths()
    assert produkt not in latest.POSSIBLE_PATHS

    latest._reveal_produkt_paths()  # run twice to check both branches
    latest._reveal_produkt_paths()
    assert produkt in latest.POSSIBLE_PATHS
