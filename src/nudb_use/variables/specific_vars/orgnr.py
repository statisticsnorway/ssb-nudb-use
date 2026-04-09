import pandas as pd
from nudb_use.datasets.nudb_data import NudbData
from nudb_use.nudb_logger import logger
from tqdm.notebook import tqdm
from nudb_use.metadata.external_apis.brreg_api import orgnr_is_underenhet


from pathlib import Path

def cleanup_orgnr_bedrift_foretak(df: pd.DataFrame, time_col_name: str, extra_orgnr_cols_split_prio: list[str] | None = None) -> pd.DataFrame:
    """Cleanup into the columns orgnrbed and orgnr_foretak using datasets from VoF.

    Args:
        df: The data we should fix.

    Returns:
        pd.DataFrame: The modified dataframe.
    """
    # Type check on time_col so we can exit early if its not something we recognize
    # Might be a year column, encoded as a string
    if pd.api.types.is_string_dtype(df[time_col_name]):
        time_col = pd.to_datetime(df[time_col_name], format="%Y")
    elif not pd.api.types.is_datetime64_any_dtype(df[time_col_name]):
        raise TypeError(f"Unrecognized datatype on {time_col_name}, should be a year-string or a datetime64.")
    else:
        time_col = df[time_col_name]
        
    orgnrbed_combine: pd.Series = pd.Series(pd.NA, index=df.index)
    orgnr_foretak_combine: pd.Series = pd.Series(pd.NA, index=df.index)

    
    cols_split_priority_order: list[str] = ["orgnr", "utd_orgnr", "orgnrbed", "orgnr_foretak"]
    if extra_orgnr_cols_split_prio is not None: 
        cols_split_priority_order += extra_orgnr_cols_split_prio

    # Split up existing columns
    for col in cols_split_priority_order:
        if col in df.columns:
            logger.info(f"Found {col} in dataframe, and splitting it and filling it into new orgnrbed and orgnr_foretak cols (first is prio).")
            orgnr_foretak_temp, orgnrbed_temp = _split_orgnr_col(df[col])
            orgnrbed_combine.fillna(orgnrbed_temp)
            orgnr_foretak_combine.fillna(orgnr_foretak_temp)

    # Join new orgnr_foretak_vof from VoF from orgnrbed - also back in time?
    orgnr_foretak_combine = _find_orgnr_foretak_vof(orgnrbed_combine, time_col).fillna(orgnr_foretak_combine)

    # Create orgnrbed_vof from cleaned orgnr_foretak where foretak is "enkeltbedriftsforetak"
    orgnrbed_combine = _find_orgnrbed_enkelbedforetak_vof(orgnr_foretak_combine, time_col).fillna(orgnr_foretak_combine)
    
    # Remove old columns
    df = df.drop(columns=cols_split_priority_order, errors="ignore")

    # Insert new columns
    df["orgnrbed"] = orgnrbed_combine
    df["orgnr_foretak"] = orgnr_foretak_combine

    return df


def _split_orgnr_col(orgnr_col: pd.Series) -> tuple[pd.Series, pd.Series]:
    vof_orgnrbed = NudbData("_vof_unique_orgnrbed").df()["orgnrbed"]
    vof_orgnr_foretak = NudbData("_vof_unique_orgnr_foretak").df()["orgnr"]
    is_bed = orgnr_col.isin(vof_orgnrbed)
    is_foretak = orgnr_col.isin(vof_orgnr_foretak)
    missing_from_vof = orgnr_col[~is_bed & ~is_foretak].dropna().unique()
    missing_orgnr_er_orgnrbed: dict[str, bool] = {}
    if missing_from_vof:
        logger.info(f"Looking for {len(missing_from_vof)} orgnr in brregs API because the orgnr are missing from the VOF-sittuttak.")
        for nr in tqdm(missing_from_vof):
            missing_orgnr_er_orgnrbed[nr] = orgnr_is_underenhet(nr)
    orgnr_is_orgnrbed = (
        missing_orgnr_er_orgnrbed | 
        {k: True for k in orgnr_col[is_bed].dropna().unique()} |
        {k: False for k in orgnr_col[is_foretak].dropna().unique()}
    )
    mask_orgnrbed = orgnr_col.map(orgnr_is_orgnrbed).astype("bool[pyarrow]")
    orgnrbed_out = pd.Series(pd.NA, index=orgnr_col.index, dtype="string[pyarrow]")
    orgnrbed_out[mask_orgnrbed] = orgnr_col
    orgnr_foretak_out = pd.Series(pd.NA, index=orgnr_col.index, dtype="string[pyarrow]")
    orgnr_foretak_out[~mask_orgnrbed]
    return orgnr_foretak_out, orgnrbed_out


def _find_orgnr_foretak_vof(orgnrbed_col: pd.Series, time_col: pd.Series) -> pd.Series:
    vof_orgnr_connections = NudbData("_vof_dated_orgnr_connections").df()
    return orgnr_fortak_col


def _find_orgnrbed_enkelbedforetak_vof(orgnr_foretak_col: pd.Series, time_col: pd.Series) -> pd.Series:
    return orgnrbed_col



