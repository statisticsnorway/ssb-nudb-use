import importlib
from types import SimpleNamespace

import pytest

get_dataset_info_module = importlib.import_module(
    "nudb_use.metadata.nudb_config.get_dataset_info"
)
from nudb_use.metadata.nudb_config.get_dataset_info import datasetname_teams_buckets
from nudb_use.metadata.nudb_config.get_dataset_info import unique_teams_buckets


@pytest.fixture()
def fake_settings() -> SimpleNamespace:
    datasets = {
        "alpha": SimpleNamespace(team="team1", bucket="bucket-a"),
        "bravo": SimpleNamespace(team="team1", bucket="bucket-b"),
        "charlie": SimpleNamespace(team="utd-vg", bucket="bucket-c"),
        "delta": SimpleNamespace(team="team2", bucket="bucket-b"),
    }
    return SimpleNamespace(datasets=datasets)


def test_datasetname_teams_buckets_default_excludes_indata(
    monkeypatch: pytest.MonkeyPatch,
    fake_settings: SimpleNamespace,
) -> None:
    monkeypatch.setattr(get_dataset_info_module, "settings", fake_settings)

    result = datasetname_teams_buckets()

    assert result == {
        "alpha": {"team": "team1", "bucket": "bucket-a"},
        "bravo": {"team": "team1", "bucket": "bucket-b"},
        "delta": {"team": "team2", "bucket": "bucket-b"},
    }


def test_datasetname_teams_buckets_includes_filter_and_no_exclude(
    monkeypatch: pytest.MonkeyPatch,
    fake_settings: SimpleNamespace,
) -> None:
    monkeypatch.setattr(get_dataset_info_module, "settings", fake_settings)

    result = datasetname_teams_buckets(
        teams_include=["team1", "utd-vg"],
        exclude_nudbs_indata=False,
    )

    assert result == {
        "alpha": {"team": "team1", "bucket": "bucket-a"},
        "bravo": {"team": "team1", "bucket": "bucket-b"},
        "charlie": {"team": "utd-vg", "bucket": "bucket-c"},
    }


def test_unique_teams_buckets_default_excludes_indata(
    monkeypatch: pytest.MonkeyPatch,
    fake_settings: SimpleNamespace,
) -> None:
    monkeypatch.setattr(get_dataset_info_module, "settings", fake_settings)

    result = unique_teams_buckets()

    assert result == {
        ("team1", "bucket-a"),
        ("team1", "bucket-b"),
        ("team2", "bucket-b"),
    }


def test_unique_teams_buckets_with_include_filter(
    monkeypatch: pytest.MonkeyPatch,
    fake_settings: SimpleNamespace,
) -> None:
    monkeypatch.setattr(get_dataset_info_module, "settings", fake_settings)

    result = unique_teams_buckets(teams_include=["team1"])

    assert result == {
        ("team1", "bucket-a"),
        ("team1", "bucket-b"),
    }
