"""Fetch and validate KLASS classification codes."""

import dateutil.parser
import klass
import pandas as pd

from nudb_use import LoggerStack
from nudb_use import logger
from nudb_use.exceptions.exception_classes import NudbQualityError
from nudb_use.exceptions.groups import raise_exception_group
from nudb_use.exceptions.groups import warn_exception_group
from nudb_use.metadata.nudb_config.get_variable_info import get_var_metadata


def get_klass_codes(
    klassid: int,
    data_time_start: str | None = None,
    data_time_end: str | None = None,
) -> list[str]:
    """Fetch code dictionaries for a classification scheme within a given time range.

    Args:
        klassid: Identifier from Klass.
        data_time_start: Start date (YYYY-MM-DD) for code filtering.
        data_time_end: End date (YYYY-MM-DD) for code filtering.

    Returns:
        list[str]: List of codes for the classification.

    Raises:
        ValueError: If data_time_end is specified, but not data_time_start.
    """
    if data_time_start is None and data_time_end is None:
        codes = klass.KlassClassification(klassid).get_codes()
    elif data_time_end is None:
        codes = klass.KlassClassification(klassid).get_codes(from_date=data_time_start)
    elif data_time_start is not None and data_time_end is not None:
        codes = klass.KlassClassification(klassid).get_codes(
            from_date=data_time_start, to_date=data_time_end
        )
    else:
        raise ValueError(
            "If you specify the end, you MUST also specify the start date (or just the start)."
        )
    return list(codes.to_dict().keys())


def check_klass_codes(
    df: pd.DataFrame,
    data_time_start: str | None = None,
    data_time_end: str | None = None,
    raise_errors: bool = True,
) -> list[NudbQualityError]:
    """Validate DataFrame columns against KLASS codelists from metadata.

    Args:
        df: DataFrame containing columns to be validated.
        data_time_start: Start date for restricting code validation, overrides metadata.
        data_time_end: End date for restricting code validation, overrides metadata.
        raise_errors: If True, raises an exception group on validation errors;
                      otherwise, only logs warnings.

    Returns:
        list[NudbQualityError]: List of quality errors detected during
        validation, empty if none.

    Raises:
        TypeError: If the type of the date found in the nudb-config dataframe does not match expectations.
    """
    with LoggerStack("Checking if column-content matches codelists in KLASS"):
        metadata = get_var_metadata()
        errors = []
        for col in df.columns:
            if col not in metadata.index:
                logger.warning(f"Not checking `{col}`, not in nudb_config!")
            elif 0 == metadata.loc[col, "klass_codelist"]:
                logger.debug(
                    f"Not checking `{col}`, its not supposed to have a codelist, the codelist-int is 0."
                )
            elif pd.isna(metadata.loc[col, "klass_codelist"]):
                logger.warning(f"Not checking `{col}`, no registered codelist!")
            else:
                logger.info(f"Checking `{col}`, found codelist ID!")
                klassid = int(metadata["klass_codelist"].astype("Int64").loc[col])

                metadata_from_date_untyped = metadata.loc[
                    col, "klass_codelist_from_date"
                ]
                if isinstance(metadata_from_date_untyped, None | str):
                    metadata_from_date_typed: str | None = metadata_from_date_untyped
                else:
                    raise TypeError(
                        f"Dont recognize the datatype of the klass from date from the config: {type(metadata_from_date_untyped)}, needs to be str | None"
                    )

                from_date, to_date = _prioritize_dates_from_param_or_config(
                    metadata_from_date_typed,
                    data_time_start,
                    data_time_end,
                )

                # What if dates are now None? Then the klass-package will default to get the latest version only...
                # Do we want to get the whole range instead?
                # first, last = _find_earliest_latest_klass_version_date(klassid)
                # from_date = first if from_date is None else from_date
                # to_date = last if to_date is None else to_date

                codelist_extras = metadata.loc[col, "codelist_extras"]

                codes = get_klass_codes(
                    klassid, data_time_start=from_date, data_time_end=to_date
                )

                if isinstance(codelist_extras, dict):
                    codes += list(codelist_extras.keys())  # type: ignore[unreachable]

                outside_df = df[(~df[col].isin(codes)) & (df[col].notna())]

                if len(outside_df):
                    errors += [
                        NudbQualityError(
                            f"Codes in {col} outside codelist: {outside_df[col].unique()}"
                        )
                    ]
                else:
                    logger.info(f"Codes from KLASS in {col} OK!")
        if raise_errors:
            raise_exception_group(errors)
        else:
            warn_exception_group(errors)

        return errors


def _prioritize_dates_from_param_or_config(
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
    else:
        to_date = None  # This will default to the codelist only being from the specified from date - what we want?

    return from_date, to_date


def _find_earliest_latest_klass_version_date(
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
