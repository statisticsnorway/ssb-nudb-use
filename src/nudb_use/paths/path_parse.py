"""Helper for parsing information out of NUDB file paths."""

import datetime
from pathlib import Path

from dapla_metadata.datasets.dapla_dataset_path_info import DaplaDatasetPathInfo


def get_periods_from_path(
    path: str | Path,
) -> None | datetime.date | tuple[datetime.date, datetime.date]:
    """Get start- and end- dates from path-string.

    This function analyzes the filename portion of the given path, extracting up to
    two period components that appear after the last '_p' in the stem.

    Args:
        path: File path as a string or Path object from which to extract period info.

    Returns:
        None | datetime.date | tuple[datetime.date, datetime.date]:
        A single period (as string or datetime) or a tuple of two periods,
        depending on how many fragments are found and whether datetime
        conversion is requested.

    Raises:
        ValueError: If there is no period parts in the path.
    """
    info = DaplaDatasetPathInfo(path)
    if info.contains_data_until is None or len(info._period_strings) > 2:
        raise ValueError(
            "All paths should have period parts mah dude, and no more than two?"
        )
    if (
        info.contains_data_from is None
        or info.contains_data_from == info.contains_data_until
    ):
        return info.contains_data_until
    return info.contains_data_from, info.contains_data_until
