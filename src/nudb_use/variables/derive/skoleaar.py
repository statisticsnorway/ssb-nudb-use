"""Derivations related to school-year columns."""

from typing import Literal

import pandas as pd

from nudb_use import LoggerStack
from nudb_use import logger

PYARROW_STRING: Literal["string[pyarrow]"] = "string[pyarrow]"


def utd_skoleaar_slutt(df: pd.DataFrame) -> pd.DataFrame:
    """Derive utd_skoleaar_slutt from utd_skoleaar_start - just adding one brah, chill out.

    Args:
        df: The dataframe to attempt to insert utd_skoleaar_slutt into.

    Returns:
        pd.DataFrame: The dataframe with utd_skoleaar_inserted if we found utd_skoleaar_start.
    """
    with LoggerStack("Attempting to derive utd_skoleaar_slutt from utd_skoleaar_start"):
        if "utd_skoleaar_start" not in df.columns:
            logger.error(
                "Cant find utd_skoleaar_start in the columns of the dataframe, cant derive utd_skoleaar_start"
            )
            return df

        # Lets only operate on valid aar
        valid_mask = (
            df["utd_skoleaar_start"].astype(PYARROW_STRING).str.len() == 4
        ) & (df["utd_skoleaar_start"].astype(PYARROW_STRING).str.isdigit())

        # Fill non valid with a placeholder
        placeholder_year = "1000"
        temp_start = df["utd_skoleaar_start"].copy()
        temp_start.loc[~valid_mask] = pd.Series(placeholder_year, index=df.index)

        df["utd_skoleaar_slutt"] = (temp_start.astype("Int64") + 1).astype(
            PYARROW_STRING
        )

        # Empty the invalid values from the newly created column
        df.loc[~valid_mask, "utd_skoleaar_slutt"] = pd.NA

        return df
