from typing import Literal

import pandas as pd

from .derive_decorator import wrap_derive

PYARROW_STRING: Literal["string[pyarrow]"] = "string[pyarrow]"


@wrap_derive
def utd_skoleaar_slutt(  # noqa: DOC101,DOC103,DOC201,DOC203
    df: pd.DataFrame,
) -> pd.Series:
    """Derive utd_skoleaar_slutt from utd_skoleaar_start.

    Raises:
        ValueError: If the dataset has weirdly formatted utd_skoleaar_start.
    """
    # We consider 4-digit strings to be valid, or empty cells
    valid_mask = (
        (df["utd_skoleaar_start"].astype(PYARROW_STRING).str.len() == 4)
        & (df["utd_skoleaar_start"].astype(PYARROW_STRING).str.isdigit())
    ) | (df["utd_skoleaar_start"].isna())
    if not valid_mask.all():
        unique_invalid = df[~valid_mask]["utd_skoleaar_start"].unique()
        err = f"There are values in the column for utd_skoleaar_start that do not match our expectations: {unique_invalid}"
        raise ValueError(err)

    return (df["utd_skoleaar_start"].astype("Int64") + 1).astype("string[pyarrow]")
