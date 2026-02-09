import pandas as pd
from typing import TypeGuard

from nudb_use.metadata.nudb_config.map_get_dtypes import BOOL_DTYPE_NAME
from nudb_use.metadata.nudb_config.map_get_dtypes import DTYPE_MAPPINGS

BOOL_DTYPE = DTYPE_MAPPINGS["pandas"][BOOL_DTYPE_NAME]
ALLNUMERIC_7DIGIT_THRESHOLD_PERCENT = 5.0

from nudb_use.nudb_logger import LoggerStack
from nudb_use.nudb_logger import logger

from .derive_decorator import wrap_derive

__all__ = ["snr_mrk"]




@wrap_derive
def snr_mrk(  # noqa:DOC201
    df: pd.DataFrame,
) -> pd.Series:
    """Derive the column snr_mrk from snr-column, True if values in snr_col is notna, has a length of 7 and are wholly alphanumeric."""
    with LoggerStack("Looking for weird content in the snr column"):
        if df["snr"].str.contains(" ").any():
            logger.warning("Some of your snr contain spaces, why bro?")

        # If there is above the threshold percent snr that are 7 digit snr that contain only numbers, give a warning.
        # Would indicate that the snrs are not pseudonomized, or that they might be cut off fake snr, probably not UUID because of the hyphens.
        allnumber_7_digits = (df["snr"].str.len() == 7) & df["snr"].str.isnumeric()
        percent = round((allnumber_7_digits.sum() / len(df) * 100), 2)
        if percent > ALLNUMERIC_7DIGIT_THRESHOLD_PERCENT:
            logger.warning(
                f"We found {percent}% rows where snr is 7 characters, but contain all digits. This is highly suspicious if you are working with pseudonomized data... Did you cut the column down to 7 characters by mistake somewhere?"
            )

    with LoggerStack("Deriving snr_mrk from snr."):
        # This function helps mypy realize that we might reach isascii
        def is_str(x: object) -> TypeGuard[str]:
            return isinstance(x, str)

        snr_mrk: pd.Series = (
            (df["snr"].notna())
            & (df["snr"].str.strip().str.len() == 7)
            & (df["snr"].str.strip().str.isalnum())
            & (
                df["snr"]
                .str.strip()
                .apply(lambda x: is_str(x) and x.isascii())
                .astype(BOOL_DTYPE)
            )  # Workaround because isascii is not supported in earlier versions of pandas, isascii fails on NAtypes?
        ).astype(BOOL_DTYPE)

        logger.info(
            f"{round(snr_mrk.sum() / len(snr_mrk)*100, 2)}%: {snr_mrk.sum()} of {len(snr_mrk)} rows have valid snr -> snr_mrk."
        )

        return snr_mrk
