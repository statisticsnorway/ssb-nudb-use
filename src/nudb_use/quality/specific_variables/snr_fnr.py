"""Checks for required personal identifier columns."""

import pandas as pd

from nudb_use.exceptions.exception_classes import NudbQualityError
from nudb_use.nudb_logger import LoggerStack
from nudb_use.nudb_logger import logger

VALID_PERSONAL_IDS_PRIO = ["snr", "fnr"]


def check_has_personal_ids(
    df: pd.DataFrame, **kwargs: object
) -> list[NudbQualityError]:
    """Ensure at least one personal identifier column is populated per row.

    Args:
        df: DataFrame containing personal identifier columns.
        **kwargs: Placeholder for future options. Passed in from parent function.

    Returns:
        list[NudbQualityError]: Errors for rows missing all identifier values.
    """
    with LoggerStack(
        f"Checking if any rows are missing all personal ids across: {VALID_PERSONAL_IDS_PRIO}"
    ):
        errors: list[NudbQualityError] = []

        found_col_names = [col for col in VALID_PERSONAL_IDS_PRIO if col in df.columns]
        if found_col_names:
            missing_ident_mask = df[found_col_names].isna().all(axis=0)
            if missing_ident_mask.any():
                number_missing = missing_ident_mask.sum()
                err_msg = f"Found {number_missing} rows where there are no personal idents in existing columns {found_col_names}, that is probably a bad idea."
                logger.warning(err_msg)
                errors.append(NudbQualityError(err_msg))
        else:
            logger.info(
                f"Found no personal identifier columns out of these: {VALID_PERSONAL_IDS_PRIO} in the dataset - should this be the case?"
            )
        return errors
