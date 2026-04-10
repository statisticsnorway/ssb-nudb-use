from typing import Any
from typing import Literal

import dateutil.parser
import klass
import pandas as pd

from nudb_use.exceptions.exception_classes import NudbQualityError
from nudb_use.nudb_logger import logger


def _outside_codes_handeling(
    series: pd.Series, codes: set[str], col: str
) -> list[NudbQualityError]:
    outside_codes = series[(~series.isin(codes)) & (series.notna())]
    if len(outside_codes):
        return [
            NudbQualityError(
                f"Codes in {col} outside codelist: {outside_codes.unique()}"
            )
        ]
    logger.info(f"Codes from KLASS in {col} OK!")
    return []


def find_earliest_latest_klass_version_date(
    klass_classification_id: int,
) -> tuple[str, str]:
    """Finds the earliest and latest version dates for a KLASS classification.

    Retrieves all versions of a given KLASS classification and identifies the
    earliest and latest dates when the classification was valid. Used to
    determine the full historical range of a classification's validity.

    Args:
        klass_classification_id: The numeric ID of the KLASS classification
            to query.

    Returns:
        tuple[str, str]: A `(min_date, max_date)` tuple representing the earliest
        and latest valid dates for the classification versions.
    """
    min_date: str = ""
    max_date: str = ""
    for version in klass.KlassClassification(klass_classification_id).versions:
        valid_from = version["validFrom"]
        if not min_date:
            min_date = valid_from
        else:
            min_date = sorted([min_date, valid_from])[0]
        if not max_date:
            max_date = valid_from
        else:
            max_date = sorted([max_date, valid_from])[-1]
    return min_date, max_date


def _resolve_date_range(
    klassid: int,
    klass_codelist_from_date: object,
    data_time_start: str | None,
    data_time_end: str | None,
) -> tuple[str | None, str | None]:
    """Pick dates from parameters over metadata, validating type."""
    metadata_from_date = _ensure_optional_str(klass_codelist_from_date)
    return _prioritize_dates_from_param_or_config(
        klassid, metadata_from_date, data_time_start, data_time_end
    )


def _include_codelist_extras(codes: list[str], codelist_extras: object) -> list[str]:
    """Include extra codes from metadata when provided."""
    if isinstance(codelist_extras, dict):
        return codes + list(codelist_extras.keys())
    return codes


def _ensure_optional_str(value: object) -> str | None:
    """Ensure a metadata value is either str or None."""
    if isinstance(value, (str, type(None))):
        return value
    raise TypeError(
        f"Dont recognize the datatype of the klass from date from the config: {type(value)}, needs to be str | None"
    )


def _truthy(val: Any) -> bool:
    if val and not pd.isna(val):
        return True
    return False


def _center_adjust(
    date1: str | None, date2: str | None, how: Literal["from"] | Literal["to"] = "from"
) -> str | None:
    # Make datetimes we can compare
    if date1:
        prio = dateutil.parser.parse(date1)
    if date2:
        second = dateutil.parser.parse(date2)

    # Enable the "how" adjustment by making two different tests towards center of time
    first_test: bool = False
    second_test: bool = False
    if _truthy(date1) and _truthy(date2) and how == "from":
        first_test = second <= prio
        second_test = prio < second
    elif _truthy(date1) and _truthy(date2) and how == "to":
        first_test = second >= prio
        second_test = prio > second

    # Record result of comparisons to the date variable
    date: str | None = None
    if first_test:
        date = prio.strftime(r"%Y-%m-%d")
    elif second_test:
        date = second.strftime(r"%Y-%m-%d")
    elif _truthy(date1):
        date = prio.strftime(r"%Y-%m-%d")
    elif _truthy(date2):
        date = second.strftime(r"%Y-%m-%d")

    return date


def _prioritize_dates_from_param_or_config(
    klassid: int,
    klass_codelist_from_date: str | None = None,
    data_time_start: str | None = None,
    data_time_end: str | None = None,
) -> tuple[str | None, str | None]:
    """Pick from/to dates for KLASS API constrained to what the classification supports.

    Args:
        klassid: KLASS classification id.
        klass_codelist_from_date: Configured start date for the codelist, if any.
        data_time_start: Requested start date (YYYY-MM-DD).
        data_time_end: Requested end date (YYYY-MM-DD).

    Returns:
        tuple[str | None, str | None]: (from_date, to_date) in YYYY-MM-DD, or (None, None) if no date constraints apply.
    """
    first_available_date, last_available_date = find_earliest_latest_klass_version_date(
        klassid
    )

    # We are prioritizing a from date first_available_date > klass_codelist_from_date > data_time_start
    # The one that is filled and of a greater date (later start date) wins.
    from_date = _center_adjust(klass_codelist_from_date, data_time_start, how="from")
    from_date = _center_adjust(first_available_date, from_date, how="from")

    # We are prioritizing a to date last_available_data < data_time_end
    # So if the last available date is earlier than the data_time_end it wins
    to_date = _center_adjust(last_available_date, data_time_end, how="to")

    # What do we do if to_date is earlier than from_date?
    to_date = _center_adjust(from_date, to_date, how="from")

    # What do we do if the dates now have passed the available date range?
    from_date = _center_adjust(from_date, last_available_date, how="to")
    to_date = _center_adjust(to_date, first_available_date, how="from")

    logger.info(f"Prioritized dates for klass: {from_date=}, {to_date=}.")
    logger.debug(
        f"From date based on: {klass_codelist_from_date=}, {data_time_start=}, {first_available_date=}"
    )
    logger.debug(f"To date based on: {data_time_end=}, {last_available_date=}")

    # Klass-api does not like when both are sent, and are the same date
    if from_date == to_date:
        return from_date, None

    return from_date, to_date
