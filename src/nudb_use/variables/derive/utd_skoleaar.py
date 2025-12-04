import pandas as pd

from .function_factory import wrap_derive_function


@wrap_derive_function
def utd_skoleaar_slutt(df: pd.DataFrame) -> pd.Series:
    """Derive utd_skoleaar_slutt from utd_skoleaar_start - just adding one brah, chill out.""" # Just need title (and optionally description)

    return (df["utd_skoleaar_start"].astype("Int64") + 1).astype("string[pyarrow]")
