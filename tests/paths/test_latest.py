from pathlib import Path

from nudb_use.paths import latest


def test_find_file_custom_dir(tmp_path: Path) -> None:
    semipath = tmp_path / "local" / "nudb-data"
    fullpath = semipath / "klargjorte-data"
    fullpath.mkdir(parents=True)

    filepath = fullpath / "tmp.parquet"
    filepath.write_text("hello world!", encoding="utf-8")

    latest._add_delt_path(semipath)
    result = latest._get_available_files()[0]

    assert result.name == "tmp.parquet"


def test_latest_shared_paths_builds_mapping(tmp_path: Path) -> None:
    base = tmp_path / "mount"
    (base / "klargjorte-data").mkdir(parents=True)

    latest._add_delt_path(base)
    file_a = base / "klargjorte-data" / "a_p2021-01-01_v1.parquet"
    file_b = base / "klargjorte-data" / "b_p2021-02-02_v2.parquet"
    file_a.write_text("a")
    file_b.write_text("b")

    paths = latest.latest_shared_paths()

    assert paths == {"a": file_a, "b": file_b}
    assert latest.latest_shared_paths("a") == file_a
