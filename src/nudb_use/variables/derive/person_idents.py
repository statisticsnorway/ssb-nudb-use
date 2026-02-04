import pandas as pd

from nudb_use.metadata.nudb_config.map_get_dtypes import BOOL_DTYPE_NAME
from nudb_use.metadata.nudb_config.map_get_dtypes import DTYPE_MAPPINGS

BOOL_DTYPE = DTYPE_MAPPINGS["pandas"][BOOL_DTYPE_NAME]


from .derive_decorator import wrap_derive

__all__ = ["snr_mrk"]


@wrap_derive
def snr_mrk(  # noqa: DOC101,DOC103,DOC201,DOC203
    df: pd.DataFrame,
) -> pd.Series:
    """Derive the column snr_mrk from snr-column, True if values in snr_col is notna, has a length of 7 and are wholly alphanumeric."""
    df["snr_mrk"] = (
        (df["snr"].notna()) & (df["snr"].str.len() == 7) & (df["snr"].str.isalnum())
    ).astype(BOOL_DTYPE)
    return df
