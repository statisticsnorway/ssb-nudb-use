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
        str | datetime.datetime | tuple[str, str] | tuple[datetime.datetime, datetime.datetime]:
        A single period (as string or datetime) or a tuple of two periods,
        depending on how many fragments are found and whether datetime
        conversion is requested.

    Raises:
        ValueError: If we cant find a valid period fragment in the path.
    """
    pathp = Path(path)
    parts = [p.split("_")[0].split(".")[0] for p in pathp.stem.split("_p")[-2:]]
    final_parts = [p for p in parts if p.replace("-", "").isdigit()]
    if not final_parts:
        raise ValueError(f"No valid period fragments found in path '{path}'.")

    if return_datetime:
        dt_parts = [date_parse(part) for part in final_parts]
        if len(dt_parts) == 1:
            return dt_parts[0]
        return (dt_parts[0], dt_parts[1])

    if len(final_parts) == 1:
        return final_parts[0]
    return (final_parts[0], final_parts[1])
