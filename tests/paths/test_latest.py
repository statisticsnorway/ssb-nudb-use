from pathlib import Path

import pytest

from nudb_use.paths import latest


def test_find_delt_path_prefers_local_when_external_missing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(latest, "UTDANNING_SHARED_EXTERNAL", tmp_path / "external")
    monkeypatch.setattr(
        latest, "UTDANNING_SHARED_LOCAL", tmp_path / "local" / "nudb-data"
    )

    (tmp_path / "local" / "nudb-data").mkdir(parents=True)

    assert latest.find_delt_path() == tmp_path / "local" / "nudb-data"


def test_filter_out_periods_paths_strips_version_and_period() -> None:
    result = latest.filter_out_periods_paths(Path("dataset_p2020-01-01_v3.parquet"))

    assert result == "dataset_p2020-01-01"


def test_latest_shared_paths_builds_mapping(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    base = tmp_path / "mount"
    (base / "klargjort-data").mkdir(parents=True)

    file_a = base / "klargjort-data" / "a_p2021-01-01_v1.parquet"
    file_b = base / "klargjort-data" / "b_p2021-02-02_v2.parquet"
    file_a.write_text("a")
    file_b.write_text("b")

    def fake_get_latest(paths: list[Path]) -> list[Path]:
        # Simulate version selection by returning the incoming list unchanged
        return paths

    monkeypatch.setattr(latest, "find_delt_path", lambda: base)
    monkeypatch.setattr(latest, "get_latest_fileversions", fake_get_latest)

    paths = latest.latest_shared_paths()

    assert paths == {"a_p2021-01-01": file_a, "b_p2021-02-02": file_b}
    assert latest.latest_shared_paths("a_p2021-01-01") == file_a
