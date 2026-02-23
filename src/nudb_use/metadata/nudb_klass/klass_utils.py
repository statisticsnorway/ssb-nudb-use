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


def _left_adjust_from(date1: str, date2: str) -> str | None:
    param_start = dateutil.parser.parse(date1)
    first_start = dateutil.parser.parse(date2)
    if param_start > first_start:
        from_date: str | None = param_start.strftime(r"%Y-%m-%d")
    else:
        from_date = first_start.strftime(r"%Y-%m-%d")
    return from_date


def _right_adjust_to(date1: str, date2: str) -> str | None:
    param_to = dateutil.parser.parse(date1)
    first_to = dateutil.parser.parse(date2)
    if param_to > first_to:
        from_date: str | None = first_to.strftime(r"%Y-%m-%d")
    else:
        from_date = param_to.strftime(r"%Y-%m-%d")
    return from_date


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

    Raises:
        ValueError: If the requested window does not overlap the classification availability window.
        TypeError: If klass_codelist_from_date has an unexpected type.
    """
    first_available_date, last_available_date = find_earliest_latest_klass_version_date(
        klassid
    )

    if data_time_start is not None:
        from_date = _left_adjust_from(data_time_start, first_available_date)
    else:
        if klass_codelist_from_date and not pd.isna(klass_codelist_from_date):
            if isinstance(klass_codelist_from_date, str):
                from_date = _left_adjust_from(
                    klass_codelist_from_date, first_available_date
                )
            else:
                raise TypeError("Unknown type of from_date here?")
        else:
            from_date = None

    if data_time_end and not pd.isna(data_time_end):
        to_date = _right_adjust_to(data_time_end, last_available_date)
    elif from_date is not None and data_time_end is None:
        to_date = last_available_date
    else:
        to_date = None

    # critical guard: if both are set, ensure overlap / valid range
    if from_date is not None and to_date is not None:
        from_dt = dateutil.parser.parse(from_date)
        to_dt = dateutil.parser.parse(to_date)
        if from_dt > to_dt:
            raise ValueError(
                f"Requested data window [{data_time_start}, {data_time_end}] does not overlap "
                f"KLASS klassid={klassid} availability [{first_available_date}, {last_available_date}]. "
                f"Computed from={from_date} to={to_date}."
            )

    return from_date, to_date
