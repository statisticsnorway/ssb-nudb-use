"""Fetch and validate KLASS classification codes."""

import datetime

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
) -> list[dict[str, str]]:
    """Fetch code dictionaries for a classification scheme within a given time range.

    Args:
        klassid: Identifier from Klass.
        data_time_start: Start date (YYYY-MM-DD) for code filtering.
        data_time_end: End date (YYYY-MM-DD) for code filtering.

    Returns:
        list[dict[str, str]]: List of dictionaries of codes for the classification.

    Raises:
        ValueError: If data_time_end is specified, but not data_time_start.
        ValueError: If both data_time_start and data_time_end are unspecified.
    """
    if data_time_start is None and data_time_end is None:
        codes = klass.KlassClassification(klassid).get_codes()
    elif data_time_end is None:
        codes = klass.KlassClassification(klassid).get_codes(from_date=data_time_start)
    elif data_time_start is not None and data_time_end is not None:
        codes = klass.KlassClassification(klassid).get_codes(
            from_date=data_time_start, to_date=data_time_end
        )
    elif data_time_start is None:
        raise ValueError(
            "If you specify the end, you MUST also specify the start date (or just the start)."
        )
    else:
        raise ValueError("What the hell are you doing with the date parameters?")
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
    """
    with LoggerStack("Checking if column-content matches codelists in KLASS"):
        metadata = get_var_metadata()
        errors = []
        for col in df.columns:
            if col not in metadata.index:
                logger.warning(f"Not checking `{col}´, not in nudb_config!")
            elif 0 == metadata.loc[col, "klass_codelist"]:
                logger.debug(
                    f"Not checking `{col}`, its not supposed to have a codelist, the codelist-int is 0."
                )
            elif pd.isna(metadata.loc[col, "klass_codelist"]):
                logger.warning(f"Not checking `{col}`, no registered codelist!")
            else:
                logger.info(f"Checking `{col}`, found codelist ID!")
                klassid = int(metadata.loc[col, "klass_codelist"])
                from_date = metadata.loc[col, "klass_codelist_from_date"]
                to_date = (
                    None
                    if from_date is None
                    else datetime.datetime.now().strftime("%Y-%m-%d")
                )
                codelist_extras = metadata.loc[col, "codelist_extras"]

                codes = get_klass_codes(
                    klassid, data_time_start=from_date, data_time_end=to_date
                )

                if isinstance(codelist_extras, dict):
                    codes += list(codelist_extras.keys())

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
