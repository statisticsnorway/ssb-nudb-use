from pytest import MonkeyPatch

from nudb_use.metadata.nudb_klass import klass_utils


def test_prioritize_dates_klass_bounds_override_params(
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        klass_utils,
        "find_earliest_latest_klass_version_date",
        lambda _id: ("2000-01-01", "2025-11-01"),
    )

    result = klass_utils._prioritize_dates_from_param_or_config(
        36,
        klass_codelist_from_date=None,
        data_time_start="1974-01-01",
        data_time_end="2026-01-01",
    )

    assert result == ("2000-01-01", "2025-11-01")
