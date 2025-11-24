import datetime
from pathlib import Path

import pytest

from nudb_use.paths import path_parse


def test_get_periods_from_path_single() -> None:
    assert path_parse.get_periods_from_path("x_p2020-01-01.parquet") == "2020-01-01"


def test_get_periods_from_path_range() -> None:
    result = path_parse.get_periods_from_path("y_p2020-01-01_p2021-02-02.parquet")

    assert result == ("2020-01-01", "2021-02-02")


def test_get_periods_from_path_datetime() -> None:
    result = path_parse.get_periods_from_path(
        Path("z_p2020-01-01.parquet"), return_datetime=True
    )

    assert isinstance(result, datetime.datetime)
    assert result.year == 2020 and result.month == 1 and result.day == 1


def test_get_periods_from_path_raises_without_period() -> None:
    with pytest.raises(ValueError):
        path_parse.get_periods_from_path("no_period_here.parquet")
