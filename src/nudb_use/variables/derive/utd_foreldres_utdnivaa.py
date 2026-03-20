import pandas as pd
from nudb_config import settings

from nudb_use.datasets.nudb_data import NudbData
from nudb_use.variables.derive.derive_decorator import wrap_derive

__all__ = [
    "utd_foreldres_utdnivaa_16aar",
    "utd_foreldres_utdnivaa_16aar_nus2000",
    "utd_hoeyeste_far_nus2000",
    "utd_hoeyeste_mor_nus2000",
]


@wrap_derive
def utd_foreldres_utdnivaa_16aar(df: pd.DataFrame) -> pd.DataFrame:
    """Derive `utd_foreldres_utdnivaa_16aar`."""
    sosbak_map = {  # Matches this codelist in klass https://www.ssb.no/klass/klassifikasjoner/227/koder
        "0": "4",
        "1": "4",
        "2": "4",
        "3": "3",
        "4": "3",
        "5": "3",
        "6": "2",
        "7": "1",
        "8": "1",
    }
    df["utd_foreldres_utdnivaa_16aar"] = (
        df["utd_foreldres_utdnivaa_16aar_nus2000"].str[0].map(sosbak_map).fillna("9")
    )
    return df


@wrap_derive
def utd_foreldres_utdnivaa_16aar_nus2000(df: pd.DataFrame) -> pd.DataFrame:
    """Derive `utd_foreldres_utdnivaa_16aar_nu2000`."""
    merge_keys = (
        settings.variables.utd_foreldres_utdnivaa_16aar_nus2000.derived_join_keys
    )

    utd_foreldres_utdnivaa_df = (
        NudbData("utd_foreldres_utdnivaa")
        .select("DISTINCT snr, utd_foreldres_utdnivaa_16aar_nus2000")
        .df()
    )

    return df.merge(
        utd_foreldres_utdnivaa_df, on=merge_keys, how="left", validate="m:1"
    )


@wrap_derive
def utd_hoeyeste_mor_nus2000(df: pd.DataFrame) -> pd.DataFrame:
    """Derive `utd_hoeyeste_mor_nus2000`."""
    merge_keys = settings.variables.utd_hoeyeste_mor_nus2000.derived_join_keys

    utd_mor_nus2000_df = (
        NudbData("utd_foreldres_utdnivaa")
        .select("DISTINCT snr, utd_hoeyeste_mor_nus2000")
        .df()
    )
    return df.merge(utd_mor_nus2000_df, on=merge_keys, how="left", validate="m:1")


@wrap_derive
def utd_hoeyeste_far_nus2000(df: pd.DataFrame) -> pd.DataFrame:
    """Derive `utd_hoeyeste_far_nus2000`."""
    merge_keys = settings.variables.utd_hoeyeste_far_nus2000.derived_join_keys

    utd_mor_nus2000_df = (
        NudbData("utd_foreldres_utdnivaa")
        .select("DISTINCT snr, utd_hoeyeste_far_nus2000")
        .df()
    )
    return df.merge(utd_mor_nus2000_df, on=merge_keys, how="left", validate="m:1")
