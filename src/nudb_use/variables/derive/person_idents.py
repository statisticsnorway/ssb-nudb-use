import pandas as pd

from nudb_use.metadata.nudb_config.map_get_dtypes import BOOL_DTYPE_NAME
from nudb_use.metadata.nudb_config.map_get_dtypes import DTYPE_MAPPINGS

BOOL_DTYPE = DTYPE_MAPPINGS["pandas"][BOOL_DTYPE_NAME]
ALLNUMERIC_7DIGIT_THRESHOLD_PERCENT = 5.0

from nudb_use.nudb_logger import LoggerStack
from nudb_use.nudb_logger import logger

from .derive_decorator import wrap_derive

__all__ = ["snr_mrk"]


@wrap_derive
def snr_mrk(  # noqa: DOC101,DOC103,DOC201,DOC203
    df: pd.DataFrame,
) -> pd.Series:
    """Derive the column snr_mrk from snr-column, True if values in snr_col is notna, has a length of 7 and are wholly alphanumeric."""
    with LoggerStack("Deriving snr_mrk from snr."):
        df["snr_mrk"] = (
            (df["snr"].notna())
            & (df["snr"].str.len() == 7)
            & (df["snr"].str.isalnum())
            & (df["snr"].apply(lambda x: x.isascii())) # Workaround because isascii is not supported in earlier versions of pandas
        ).astype(BOOL_DTYPE)

        # If there is above the threshold percent snr that are 7 digit snr that contain only numbers, give a warning.
        # Would indicate that the snrs are not pseudonomized, or that they might be cut off fake snr, probably not UUID because of the hyphens.
        allnumber_7_digits = (df["snr"].str.len() == 7) & df["snr"].str.isnumeric()
        percent = round((allnumber_7_digits.sum() / len(df) * 100), 2)
        if percent > ALLNUMERIC_7DIGIT_THRESHOLD_PERCENT:
            logger.warning(
                f"We found {percent}% rows where snr is 7 characters, but contain all digits. This is highly suspicious if you are working with pseudonomized data... Did you cut the column down to 7 characters by mistake somewhere?"
            )

        return df
