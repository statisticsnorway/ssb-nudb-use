"""Validations for kommune-coded variables referencing KLASS 131."""

import pandas as pd
from nudb_config import settings

from nudb_use.exceptions.exception_classes import NudbQualityError
from nudb_use.nudb_logger import LoggerStack
from nudb_use.nudb_logger import logger

from .utils import add_err2list
from .utils import get_column
from .utils import require_series_present

KOMMUNE_VARS = [
    var_name
    for var_name, var_details in settings.variables.items()
    if var_details.get("klass_codelist") == 131
]


def check_kommune(df: pd.DataFrame, **kwargs: object) -> list[NudbQualityError]:
    """Run kommune-specific validations on all kommune columns in the DataFrame.

    Args:
        df: DataFrame that may contain kommune columns.
        **kwargs: Placeholder for future configuration. Passed in from parent function.

    Returns:
        list[NudbQualityError]: Errors aggregated from kommune checks.
    """
    kommune_vars_in_df = [col for col in df.columns.str.lower() if col in KOMMUNE_VARS]
    errors: list[NudbQualityError] = []
    if not kommune_vars_in_df:
        return errors
    with LoggerStack(
        f"Validating specific kommune-variables that link to klass-codelist 131: {kommune_vars_in_df}"
    ):
        for kom_col_name in kommune_vars_in_df:
            kom_col = get_column(df, col=kom_col_name)
            add_err2list(
                errors,
                subcheck_single_kommune_oslo_svalbard_utland(kom_col),
            )
            add_err2list(
                errors,
                subcheck_only_single_sentinel_value_9999_allowed(kom_col),
            )
        return errors


def subcheck_single_kommune_oslo_svalbard_utland(
    kommune_col: pd.Series | None,
) -> NudbQualityError | None:
    """Ensure fylker with single municipality codes are mapped correctly.

    Args:
        kommune_col: Series with kommune codes.

    Returns:
        NudbQualityError | None: Error when illegal mappings exist, else None.
    """
    validated = require_series_present(kommune_col=kommune_col)
    if validated is None:
        return None
    kommune_col = validated["kommune_col"]

    legal_vals = {
        "03": "0301",
        "21": "2111",
        "25": "2580",
    }

    unique_vals = pd.Series(kommune_col.unique())
    unique_vals_in_legal = unique_vals[unique_vals.str[:2].isin(legal_vals.keys())]
    non_match = unique_vals_in_legal[
        unique_vals_in_legal != unique_vals_in_legal.str[:2].map(legal_vals)
    ]

    if not len(unique_vals_in_legal):
        return None

    non_match_dict = {old: legal_vals[old[:2]] for old in non_match}
    if not non_match_dict:
        return None

    err_msg = f"Found illegal kommune-values that can be recoded because the fylke only maps to single kommune: {non_match_dict}"
    logger.warning(err_msg)
    return NudbQualityError(err_msg)


def subcheck_only_single_sentinel_value_9999_allowed(
    kommune_col: pd.Series | None,
) -> NudbQualityError | None:
    """Ensure kommune codes only use '9999' as the sentinel value.

    Args:
        kommune_col: Series with kommune codes.

    Returns:
        NudbQualityError | None: Error when other sentinel values exist, else None.
    """
    validated = require_series_present(kommune_col=kommune_col)
    if validated is None:
        return None
    kommune_col = validated["kommune_col"]
    unique_vals = pd.Series(kommune_col.dropna().unique())
    mask_weird = (
        unique_vals.str.startswith("00") | unique_vals.str.startswith("9")
    ) & (unique_vals != "9999")

    if mask_weird.sum() == 0:
        return None

    err_msg = (
        f"Found weird sentinel values in your kommune-col, here are the values: "
        f"{list(unique_vals[mask_weird])}"
    )
    logger.warning(err_msg)
    return NudbQualityError(err_msg)
