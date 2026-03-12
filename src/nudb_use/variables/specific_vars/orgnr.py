import pandas as pd
from nudb_use.datasets.nudb_data import NudbData

def cleanup_orgnr_bedrift_foretak(df: pd.DataFrame) -> pd.DataFrame:
    """Cleanup into the columns orgnrbed and orgnr_foretak using datasets from VoF.

    Args:
        df: The data we should fix.

    Returns:
        pd.DataFrame: The modified dataframe.
    """
    orgnrbed_combine: pd.Series = pd.Series(pd.NA, index=df.index)
    orgnr_foretak_combine: pd.Series = pd.Series(pd.NA, index=df.index)

    cols_split_priority_order: list[str] = ["orgnr", "utd_orgnr", "orgnrbed", "orgnr_foretak"]

    # Split up existing columns
    for col in cols_split_priority_order:
        if col in df.columns:
            orgnrbed_temp, orgnr_foretak_temp = _split_orgnr_col(df[col])
            orgnrbed_combine.fillna(orgnrbed_temp)
            orgnr_foretak_combine.fillna(orgnr_foretak_temp)

    # Join new orgnr_foretak_vof from VoF from orgnrbed - also back in time?
    orgnr_foretak_combine = _find_orgnr_foretak_vof(orgnrbed_combine).fillna(orgnr_foretak_combine)

    # Create orgnrbed_vof from cleaned orgnr_foretak where foretak is "enkeltbedriftsforetak"
    orgnrbed_combine = _find_orgnrbed_enkelbedforetak_vof(orgnr_foretak_combine).fillna(orgnr_foretak_combine)
    
    # Remove old columns
    df = df.drop(columns=cols_split_priority_order, errors="ignore")

    # Insert new columns
    df["orgnrbed"] = orgnrbed_combine
    df["orgnr_foretak"] = orgnr_foretak_combine

    return df




def _split_orgnr_col(s: pd.Series) -> tuple[pd.Series, pd.Series]:
    df_orgnrbed = NudbData("_vof_unique_orgnrbed").df()
    mask = s.isin(df_orgnrbed["orgnrbed"])
    return s[mask], s[~mask]


def _find_orgnr_foretak_vof(s: pd.Series) -> pd.Series:
    return s


def _find_orgnrbed_enkelbedforetak_vof(s: pd.Series) -> pd.Series:
    return s