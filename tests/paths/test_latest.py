from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import Mock

import pytest

from nudb_use.nudb_logger import logger
from nudb_use.paths import latest


@pytest.mark.parametrize(
    ("language", "expected"),
    [
        ("nb", "Norsk beskrivelse"),
        ("en", "English description"),
        ("nn", "English description"),
    ],
)
def test_report_datadoc_version_description_selects_language(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, language: str, expected: str
) -> None:
    path = tmp_path / "dataset_v1.parquet"
    metadata = Mock()
    metadata.dataset.version_description.root = [
        SimpleNamespace(languageCode="en", languageText="English description"),
        SimpleNamespace(languageCode="nb", languageText="Norsk beskrivelse"),
    ]
    datadoc = Mock(return_value=metadata)
    monkeypatch.setattr(latest, "Datadoc", datadoc)

    result = latest._report_datadoc_version_description(path, language=language)

    assert result == expected
    datadoc.assert_called_once_with(
        metadata_document_path=tmp_path / "dataset_v1__DOC.json"
    )


def test_report_datadoc_version_description_missing_metadata(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(
        latest, "Datadoc", Mock(side_effect=ValueError("Missing metadata"))
    )
    warning = Mock()
    monkeypatch.setattr(logger, "warning", warning)

    result = latest._report_datadoc_version_description(tmp_path / "dataset_v1.parquet")

    assert result == ""
    warning.assert_called_once()
    assert "Missing metadata" in warning.call_args.args[0]


@pytest.mark.parametrize(
    "version_description",
    [
        None,
        SimpleNamespace(root=None),
        SimpleNamespace(root=[]),
        SimpleNamespace(root=[SimpleNamespace(languageCode="nb", languageText=None)]),
        SimpleNamespace(root=[SimpleNamespace(languageCode="nb", languageText="")]),
    ],
)
def test_report_datadoc_version_description_without_text(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    version_description: SimpleNamespace | None,
) -> None:
    metadata = Mock()
    metadata.dataset.version_description = version_description
    monkeypatch.setattr(latest, "Datadoc", Mock(return_value=metadata))

    assert (
        latest._report_datadoc_version_description(tmp_path / "dataset.parquet") == ""
    )


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


def test_hide_reveal_produkt_path(monkeypatch: Any) -> None:
    # patch
    monkeypatch.setattr(latest, "POSSIBLE_PATHS", latest.ALL_POSSIBLE_PATHS)

    produkt = Path(latest.PRODUKT)
    assert produkt in latest.POSSIBLE_PATHS

    latest._hide_produkt_paths()  # run twice to check both branches
    latest._hide_produkt_paths()
    assert produkt not in latest.POSSIBLE_PATHS

    latest._reveal_produkt_paths()  # run twice to check both branches
    latest._reveal_produkt_paths()
    assert produkt in latest.POSSIBLE_PATHS
