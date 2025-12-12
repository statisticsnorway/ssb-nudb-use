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


def _prioritize_dates_from_param_or_config(
    klassid: int,
    klass_codelist_from_date: str | None = None,
    data_time_start: str | None = None,
    data_time_end: str | None = None,
) -> tuple[str | None, str | None]:

    # Prioritize sent parameter
    if data_time_start is not None:
        from_date: str | None = dateutil.parser.parse(data_time_start).strftime(
            r"%Y-%m-%d"
        )
    else:
        # We dont want to convert from_date missing to a string 'None' for example
        if klass_codelist_from_date and not pd.isna(klass_codelist_from_date):
            if isinstance(klass_codelist_from_date, str):
                from_date = dateutil.parser.parse(klass_codelist_from_date).strftime(
                    r"%Y-%m-%d"
                )
            else:
                raise TypeError("Unknown type of from_date here?")
        else:
            from_date = None

    # Prioritize sent parameter
    if data_time_end and not pd.isna(data_time_end):
        to_date: str | None = dateutil.parser.parse(data_time_end).strftime(r"%Y-%m-%d")
    # Decided with pph that when we set the start date, we usually want the full range of a codelist... (?)
    # So if we have a non-None start date, we want a filled stop date, the latest available version.
    elif from_date is not None and data_time_end is None:
        first_available_date, to_date = find_earliest_latest_klass_version_date(klassid)
        if dateutil.parser.parse(first_available_date) > dateutil.parser.parse(
            from_date
        ):
            # Move the from_date to the first available date
            from_date = first_available_date
            logger.info(
                f"The sent from_date was earlier than the first available klass version date, so we changed it to: {from_date}"
            )
    else:
        to_date = None  # This will default to the codelist only being from the specified from date?

    return from_date, to_date
