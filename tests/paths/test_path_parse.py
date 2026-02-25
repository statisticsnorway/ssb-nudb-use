import datetime

import pytest

from nudb_use.paths import path_parse


def test_get_periods_from_path_single() -> None:
    assert path_parse.get_periods_from_path("x_p2020-01-01.parquet") == datetime.date(
        year=2020, month=1, day=1
    )


def test_get_periods_from_path_range() -> None:
    result = path_parse.get_periods_from_path("y_p2020-01-01_p2021-02-02.parquet")
    assert result == (
        datetime.date(year=2020, month=1, day=1),
        datetime.date(year=2021, month=2, day=2),
    )


def test_get_periods_from_path_range_3weird() -> None:
    with pytest.raises(ValueError):
        path_parse.get_periods_from_path("y_p1998_p2020_p2021.parquet")


def test_get_periods_from_path_raises_without_period() -> None:
    with pytest.raises(ValueError):
        path_parse.get_periods_from_path("no_period_here.parquet")
