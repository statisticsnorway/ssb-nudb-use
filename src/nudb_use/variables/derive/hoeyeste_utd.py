from __future__ import annotations

import pandas as pd

from .derive_decorator import wrap_derive_join_all_data

__all__ = [
    "utd_hoeyeste_nus2000",
]


@wrap_derive_join_all_data
def utd_hoeyeste_nus2000(df: pd.DataFrame) -> pd.DataFrame:
    """Derive utd_hoeyeste_nus2000 / old BU. The persons highest education according to nus2000.

    Args:
        df: Source dataset containing at least snr, nus2000, utd_fullfoertkode.

    Returns:
        pd.DataFrame: DataFrame with new column.

    Raises:
        NotImplementedError: Raises this error because the function is not finished yet.
    """
    raise NotImplementedError(
        "This is not finished BU-programming, we need to decide if we should be producing a dataset like F_UTD_DEMOGRAFI or similar."
    )
