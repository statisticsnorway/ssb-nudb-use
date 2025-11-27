from pathlib import Path

import pytest

from nudb_use.paths import path_utils


def test_next_path_mkdir_uses_next_version(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    base = tmp_path / "data" / "file_v1.parquet"

    def fake_next_version(path: str) -> str:
        assert path == str(base)
        return str(tmp_path / "data" / "file_v2.parquet")

    monkeypatch.setattr(path_utils, "next_version_path", fake_next_version)

    new_path = path_utils.next_path_mkdir(base)

    assert new_path == tmp_path / "data" / "file_v2.parquet"
    assert (tmp_path / "data").is_dir()


def test_metadatapath_from_path_builds_doc_path() -> None:
    result = path_utils.metadatapath_from_path("/tmp/a/b/file.parquet")

    assert result == Path("/tmp/a/b/file__DOC.json")
