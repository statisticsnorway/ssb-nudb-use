from pathlib import Path
from typing import Any

from nudb_use.paths import latest
from nudb_use.paths.path_date import set_nudb_date


def test_set_nudb_date(monkeypatch: Any, tmp_path: Path) -> None:
    base = tmp_path / "mount"
    (base / "klargjorte-data").mkdir(parents=True)

    monkeypatch.setattr(latest, "POSSIBLE_PATHS", [])
    latest._add_delt_path(base)

    file_a = base / "klargjorte-data" / "tmp_test_file_p2021-01-01_v1.parquet"
    file_b = base / "klargjorte-data" / "tmp_test_file_p2022-02-02_v2.parquet"
    file_c = base / "klargjorte-data" / "tmp_test_file_p2023-03-03_v2.parquet"

    file_a.write_text("a")
    file_b.write_text("b")
    file_c.write_text("c")

    def files():
        return sorted(latest._get_available_files("tmp_test_file"))

    assert files() == [file_a, file_b, file_c]

    set_nudb_date("2022-02-01")
    assert files() == [file_a]

    set_nudb_date("2022-02")
    assert files() == [file_a, file_b]

    set_nudb_date("2022")
    assert files() == [file_a, file_b]

    set_nudb_date("2021")
    assert files() == [file_a]
