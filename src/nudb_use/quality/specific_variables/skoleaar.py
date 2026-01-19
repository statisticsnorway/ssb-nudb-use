"""Validations for the skoleaar time variables."""

import pandas as pd

from nudb_use.exceptions.exception_classes import NudbQualityError
from nudb_use.nudb_logger import LoggerStack
from nudb_use.nudb_logger import logger

from .utils import add_err2list

SANE_SKOLAAR_RANGE = [1969, 2050]


def check_skoleaar(df: pd.DataFrame, **kwargs: object) -> list[NudbQualityError]:
    """Validate skoleaar columns for invalid year formats.

    Scans columns whose name includes "skoleaar" or "skolar" and aggregates
    any validation errors found.

    Args:
        df: Input DataFrame to validate.
        **kwargs: Unused extra arguments for compatibility.

    Returns:
        list[NudbQualityError]: Collected validation errors.
    """
    with LoggerStack(
        "Validating skolaar / skolear variables according to specific rules."
    ):
        skoleaar_cols: dict[str, pd.Series] = {
            c: df[c]
            for c in df.columns
            if "skoleaar" in c.lower() or "skolar" in c.lower()
        }

        errors: list[NudbQualityError] = []

        for col_name, col_series in skoleaar_cols.items():
            col_str_error = check_skoleaar_is_string_dtype(col_name, col_series)
            add_err2list(errors, col_str_error)
            # If the column is not string dtype, the following tests will fail on their assumptions
            if col_str_error is None:
                unique_years = pd.Series(col_series.unique())
                add_err2list(
                    errors, check_skoleaar_contains_one_year(col_name, unique_years)
                )
                add_err2list(
                    errors, check_skoleaar_contains_sane_years(col_name, unique_years)
                )
                add_err2list(
                    errors,
                    check_skoleaar_contains_two_years_one_offset(
                        col_name, unique_years
                    ),
                )

        return errors


def check_skoleaar_is_string_dtype(
    col_name: str, col_series: pd.Series
) -> NudbQualityError | None:
    """Ensure a skoleaar column uses a string dtype before further checks.

    Args:
        col_name: Column name for error context.
        col_series: Series of skoleaar values to validate.

    Returns:
        NudbQualityError | None: Error when dtype is not string, otherwise None.
    """
    if not pd.api.types.is_string_dtype(col_series):
        err_msg = f"`{col_name}` should be a string dtype. Can't run the other on skoleaar until you fix this. Current dtype: {col_series.dtype}"
        logger.warning(err_msg)
        return NudbQualityError(err_msg)
    return None


def check_skoleaar_contains_one_year(
    col_name: str, col_series: pd.Series
) -> NudbQualityError | None:
    """Validate that skoleaar values look like a single 4-digit year.

    Args:
        col_name: Column name for error context.
        col_series: Series of skoleaar values as strings.

    Returns:
        NudbQualityError | None: Error if invalid values are found, otherwise None.
    """
    unique_weird_values = list(
        col_series[
            (
                (col_series.notna())
                & ((~col_series.str.isdigit()) | (col_series.str.len() != 4))
            )
        ]
    )
    if unique_weird_values:
        if len(unique_weird_values) >= 10:
            err_msg = f"`{col_name}` should contain only a single year as a string dtype. But has some weird values, 10 first weird values: {unique_weird_values[:10]}"
        else:
            err_msg = f"`{col_name}` should contain only a single year as a string dtype. But has some weird values, weird values: {unique_weird_values}"
        logger.warning(err_msg)
        return NudbQualityError(err_msg)
    return None


def check_skoleaar_contains_sane_years(
    col_name: str, col_series: pd.Series
) -> NudbQualityError | None:
    """Validate that skoleaar values fall within a sane year range.

    Args:
        col_name: Column name for error context.
        col_series: Series of skoleaar values as strings or integers.

    Returns:
        NudbQualityError | None: Error if out-of-range values are found, otherwise None.
    """
    numeric_values = pd.to_numeric(col_series, errors="coerce").astype("Int64").dropna()
    outside_sane_range = list(
        numeric_values[
            (numeric_values <= SANE_SKOLAAR_RANGE[0])
            | (numeric_values > SANE_SKOLAAR_RANGE[1])
        ]
    )
    if outside_sane_range:
        if len(outside_sane_range) >= 10:
            err_msg = f"`{col_name}` should contain values between {SANE_SKOLAAR_RANGE[0]} and {SANE_SKOLAAR_RANGE[1]}, 10 first weird values: {outside_sane_range[:10]}."
        else:
            err_msg = f"`{col_name}` should contain values between {SANE_SKOLAAR_RANGE[0]} and {SANE_SKOLAAR_RANGE[1]}, weird values: {outside_sane_range}."
        logger.warning(err_msg)
        return NudbQualityError(err_msg)
    return None


def check_skoleaar_contains_two_years_one_offset(
    col_name: str, col_series: pd.Series
) -> NudbQualityError | None:
    """Validate 8-digit skoleaar values have a +1 year offset.

    Args:
        col_name: Column name for error context.
        col_series: Series of skoleaar values as strings.

    Returns:
        NudbQualityError | None: Error if 8-digit values are not consecutive, otherwise None.
    """
    two_years = col_series[col_series.str.len() == 8]
    two_years_part1 = (
        pd.to_numeric(two_years.str[:4], errors="coerce").astype("Int64").fillna(0)
    )
    two_years_part2 = (
        pd.to_numeric(two_years.str[4:], errors="coerce").astype("Int64").fillna(0)
    )
    wierd_offsets = list(two_years[(two_years_part1 + 1) != two_years_part2])
    if wierd_offsets:
        err_msg = f"`WEIRD OFFSETS: {col_name}` contains years with 8 digits (which is wrong), BUT IN ADDITION, the second year is not always one more than the first: {wierd_offsets}"
        logger.info(err_msg)
        return NudbQualityError(err_msg)
    return None
