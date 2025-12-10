"""Validations ensuring certain columns are unique per person."""

import pandas as pd

from nudb_use.exceptions.exception_classes import NudbQualityError
from nudb_use.nudb_logger import LoggerStack
from nudb_use.nudb_logger import logger

from .utils import add_err2list
from .utils import get_column
from .utils import require_series_present

UNIQUE_PER_PERSON_COLS = ["pers_kjoenn", "pers_foedselsdato", "gr_grunnskolepoeng"]


def check_unique_per_person(
    df: pd.DataFrame, **kwargs: object
) -> list[NudbQualityError]:
    """Ensure configured columns have at most one value per person.

    Args:
        df: DataFrame containing personal identifier and value columns.
        **kwargs: Placeholder for future options. Passed in from parent function.

    Returns:
        list[NudbQualityError]: Errors describing rows that violate uniqueness.
    """
    snr = get_column(df, col="snr")
    fnr = get_column(df, col="fnr")

    errors: list[NudbQualityError] = []

    unique_cols_in_df = [
        col for col in df.columns if col.lower() in UNIQUE_PER_PERSON_COLS
    ]
    if len(unique_cols_in_df):
        with LoggerStack(
            f"Checking columns that should be unique per person: {UNIQUE_PER_PERSON_COLS}"
        ):
            for unique_col in unique_cols_in_df:
                add_err2list(
                    errors,
                    subcheck_unique_per_person(fnr, snr, df[unique_col], unique_col),
                )

    return errors


def subcheck_unique_per_person(
    fnr: pd.Series | None,
    snr: pd.Series | None,
    unique_col: pd.Series | None,
    unique_col_name: str,
) -> NudbQualityError | None:
    """Check that a single column has unique values per person.

    Args:
        fnr: Series containing national identifiers.
        snr: Series containing snr person identifiers.
        unique_col: Column that should hold unique values per person.
        unique_col_name: Name of the column for logging context.

    Returns:
        NudbQualityError | None: Error when multiple values exist per person,
        else None.
    """
    validated = require_series_present(fnr=fnr, snr=snr, unique_col=unique_col)
    if validated is None:
        return None
    fnr = validated["fnr"]
    snr = validated["snr"]
    unique_col = validated["unique_col"]

    test_df = pd.DataFrame({"snr": snr.fillna(fnr), "unique_col": unique_col})
    test_mask = test_df.groupby("snr")["unique_col"].transform("nunique") > 1
    if not test_mask.sum():
        return None

    err_msg = f"Found several values per person, in {unique_col_name} that should only have a single value per person."
    logger.warning(err_msg)
    return NudbQualityError(err_msg)
