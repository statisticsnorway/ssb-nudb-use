import datetime as dt
import functools
from pathlib import Path

from dapla_metadata.datasets.dapla_dataset_path_info import DaplaDatasetPathInfo

from nudb_use.nudb_logger import logger

NUDB_DATE = None


@functools.total_ordering
class DaplaFileDate:
    """Handle resolution variable dates."""

    def __init__(self, datestr: str) -> None:
        split = [int(x) for x in datestr.split("-")]

        self.year = split[0] if len(split) > 0 else None
        self.month = split[1] if len(split) > 1 else None
        self.day = split[2] if len(split) > 2 else None

    def __eq__(self, other: "DaplaFileDate") -> bool:
        """Are dates equivalent?"""
        if self.year is None or other.year is None:
            return True  # both empty

        eq_year = self.year == other.year

        if not eq_year or self.month is None or other.month is None:
            return eq_year

        eq_month = self.month == other.month

        if not eq_month or self.day is None or other.day is None:
            return eq_month

        return self.day == other.day

    def __lt__(self, other: "DaplaFileDate") -> bool:
        """Is date less than other date?"""
        if self.year is None or other.year is None:
            return False  # both empty

        eq_year = self.year == other.year

        if not eq_year or self.month is None or other.month is None:
            return self.year < other.year

        eq_month = self.month == other.month

        if not eq_month or self.day is None or other.day is None:
            return self.month < other.month

        return self.day < other.day

    def __str__(self) -> str:
        """String representation of date."""
        return f"{self.year}-{self.month}-{self.day}"

    def __repr__(self) -> str:
        """String representation of date."""
        return self.__str__()


def _get_dapla_last_file_date_from_path(
    path: str | Path,
) -> None | DaplaFileDate:
    info = DaplaDatasetPathInfo(path)

    if not len(info.period_strings):
        raise ValueError("No period strings in file path!")

    last_date = info.period_strings[-1]
    return DaplaFileDate(last_date)


def _nudb_use_dated_paths() -> bool:
    return NUDB_DATE is not None


def set_nudb_date(
    val: dt.datetime | str | None = None, date_format: str = "%Y-%m-%d"
) -> None:
    """Set NUDB_DATE."""
    global NUDB_DATE

    if val is None:
        NUDB_DATE = None
        return

    if isinstance(val, dt.datetime):
        val = val.strftime("%Y-%m-%d")

    NUDB_DATE = DaplaFileDate(val)


def get_nudb_date() -> dt.datetime | None:
    """Get NUDB_DATE."""
    return NUDB_DATE


def _file_has_valid_date(filepath: Path, default: bool = True) -> bool:
    if not _nudb_use_dated_paths():
        return True

    try:
        date = _get_dapla_last_file_date_from_path(filepath)
        return date <= get_nudb_date()
    except Exception as err:
        logger.warning(
            f"Could not validate file date! Returning {default=}!\nMessage:{err}"
        )
        return default
