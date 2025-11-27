"""Validations for country (land) variables mapping to KLASS 91."""

import pandas as pd
from nudb_config import settings

from nudb_use.exceptions.exception_classes import NudbQualityError
from nudb_use.nudb_logger import LoggerStack

from .utils import add_err2list
from .utils import get_column
from .utils import require_series_present

LAND_VARS = [
    var_name
    for var_name, var_details in settings.variables.items()
    if var_details.get("klass_codelist") == 91
]


def check_land(df: pd.DataFrame, **kwargs: object) -> list[NudbQualityError]:
    """Run land-specific validations for all configured columns.

    Args:
        df: DataFrame containing potential land variables.
        **kwargs: Placeholder for future options. Passed in from parent function.

    Returns:
        list[NudbQualityError]: Errors aggregated from land checks.
    """
    errors: list[NudbQualityError] = []

    land_vars_in_df = [col for col in df.columns.str.lower() if col in LAND_VARS]
    if not land_vars_in_df:
        return errors

    with LoggerStack(
        f"Validating country (land) variables that link to klass codelist 91 {LAND_VARS}"
    ):
        for land_col_name in land_vars_in_df:
            land_col = get_column(df, col=land_col_name)
            add_err2list(errors, subcheck_landkode_000(land_col, land_col_name))
        return errors


def subcheck_landkode_000(
    land_col: pd.Series | None, col_name: str
) -> NudbQualityError | None:
    """Ensure the reserved land code `000` is not incorrectly used.

    Args:
        land_col: Series with land codes.
        col_name: Name of the land column for logging context.

    Returns:
        NudbQualityError | None: Error when invalid codes are present, else None.
    """
    validated = require_series_present(land_col=land_col)
    if validated is None:
        return None
    land_col = validated["land_col"]

    illegal_vals = ["000"]
    err_results = [
        val
        for val in land_col.unique()
        if pd.isna(val) and isinstance(val, str) and val in illegal_vals
    ]
    if not err_results:
        return None
    return NudbQualityError(
        f'Found landkode for Norway in {col_name}, are you sure it should be there? (Is it a valid "studieland" if that relates to the column?'
    )
