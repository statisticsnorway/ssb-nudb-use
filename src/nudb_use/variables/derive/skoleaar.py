import pandas as pd
from nudb_use import logger, LoggerStack

def utd_skoleaar_slutt(df: pd.DataFrame) -> pd.DataFrame:
    """Derive utd_skoleaar_slutt from utd_skoleaar_start - just adding one brah, chill out.

    Args:
        df: The dataframe to attempt to insert utd_skoleaar_slutt into.

    Returns:
        pd.DataFrame: The dataframe with utd_skoleaar_inserted if we found utd_skoleaar_start.
    """
    
    with LoggerStack("Attempting to derive utd_skoleaar_slutt from utd_skoleaar_start"):
        if "utd_skoleaar_start" not in df.columns:
            logger.error("Cant find utd_skoleaar_start in the columns of the dataframe, cant derive utd_skoleaar_start")
            return df

        df["utd_skoleaar_slutt"] = (df["utd_skoleaar_start"].astype("Int64") + 1).astype("string[pyarrow]")

        return df