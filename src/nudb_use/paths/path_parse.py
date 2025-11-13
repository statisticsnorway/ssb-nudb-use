"""Helper for parsing information out of NUDB file paths."""

import datetime
from pathlib import Path

from dateutil.parser import parse as date_parse


def get_periods_from_path(
    path: str | Path, return_datetime: bool = False
) -> (
    str
    | datetime.datetime
    | tuple[str, str]
    | tuple[datetime.datetime, datetime.datetime]
):
    """Get start- and end- dates from path-string.

    This function analyzes the filename portion of the given path, extracting up to
    two period components that appear after the last '_p' in the stem. Each period
    string is cleaned by splitting on underscores and dots, focusing on numeric date
    formats. Optionally, these extracted strings can be converted to datetime objects
    if `return_datetime` is True.

    Args:
        path: File path as a string or Path object from which to extract period info.
        return_datetime: If True, convert extracted period strings to datetime objects.

    Returns:
        A single period as str or datetime.datetime if one period is found.
        A tuple of two periods as strings or datetime datetimes if two periods are found.
        The exact return type depends on the number of period elements found and
        the value of `return_datetime`.

    """
    pathp = Path(path)
    parts = [p.split("_")[0].split(".")[0] for p in pathp.stem.split("_p")[-2:]]
    final_parts = [p for p in parts if p.replace("-", "").isdigit()]
    if return_datetime:
        final_parts = [date_parse(str(p)) for p in final_parts]
    return final_parts
