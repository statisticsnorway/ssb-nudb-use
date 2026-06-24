import pandas as pd
from nudb_config import settings
from numpy import dtype as np_dtype
from numpy import generic as np_generic
from pandas.api.extensions import ExtensionDtype
from pandas.api.types import pandas_dtype

from nudb_use.datasets.nudb_data import NudbData
from nudb_use.datasets.nudb_database import nudb_database
from nudb_use.nudb_logger import logger
from nudb_use.variables.derive.derive_decorator import wrap_derive

STRING_DTYPE: ExtensionDtype | np_dtype[np_generic] = pandas_dtype(
    settings.constants.datadoc_pandas_dtype_mapping.STRING
)

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


def _derive_utd_foreldres_utdnivaa_var(df: pd.DataFrame, *, varname: str) -> pd.Series:
    """Generic DuckDB derivation helper for variables from `utd_foreldres_utdnivaa`."""
    df = df.copy()

    if varname in df.columns:
        logger.warning(
            f'{varname} already exists... If `priority="old"` some values might not get updated!'
        )
        df = df.drop(columns=varname)

    df["snr"] = df["snr"].astype(STRING_DTYPE)
    sosbak = NudbData("utd_foreldres_utdnivaa")
    con = nudb_database.get_connection()
    con.register("_tmp_df", df[["snr"]].drop_duplicates())

    mapping = con.sql(f"""
        SELECT DISTINCT
            T1.snr,
            T2.{varname} AS {varname}
        FROM
            _tmp_df AS T1
        LEFT JOIN
            {sosbak.alias} AS T2
        ON
            T1.snr = T2.snr;
    """).df()

    result = df.merge(right=mapping, on="snr", how="left", validate="m:1")

    if result.shape[0] > df.shape[0]:
        logger.warning(
            f"Number of observations grew from {df.shape[0]} to {result.shape[0]}!"
        )
    elif result.shape[0] < df.shape[0]:
        logger.warning(
            f"Number of observations decreased from {df.shape[0]} to {result.shape[0]}!"
        )

    return result[varname]


@wrap_derive
def utd_foreldres_utdnivaa_16aar_nus2000(df: pd.DataFrame) -> pd.Series:
    """Derive `utd_foreldres_utdnivaa_16aar_nus2000`."""
    return _derive_utd_foreldres_utdnivaa_var(
        df, varname="utd_foreldres_utdnivaa_16aar_nus2000"
    )


@wrap_derive
def utd_hoeyeste_mor_nus2000(df: pd.DataFrame) -> pd.Series:
    """Derive `utd_hoeyeste_mor_nus2000`."""
    return _derive_utd_foreldres_utdnivaa_var(df, varname="utd_hoeyeste_mor_nus2000")


@wrap_derive
def utd_hoeyeste_far_nus2000(df: pd.DataFrame) -> pd.Series:
    """Derive `utd_hoeyeste_far_nus2000`."""
    return _derive_utd_foreldres_utdnivaa_var(df, varname="utd_hoeyeste_far_nus2000")
