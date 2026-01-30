import pandas as pd

from .derive_decorator import wrap_derive

__all__ = ["utd_erutland"]


@wrap_derive
def utd_erutland(  # noqa: DOC101,DOC103,DOC201,DOC203
    df: pd.DataFrame,
) -> pd.Series:
    """Derive utd_erutland from utd_skolekom."""
    utd_erutland: pd.Series = (
        df["utd_skolekom"]
        .isin(["0025", "1025", "2025", "2400", "2580"])
        .astype("bool[pyarrow]")
    )
    return utd_erutland
