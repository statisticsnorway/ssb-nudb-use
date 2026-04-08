import pandas as pd
from nudb_config import settings

from nudb_use.datasets.nudb_data import NudbData
from nudb_use.datasets.nudb_database import nudb_database
from nudb_use.nudb_logger import logger
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


def _derive_utd_foreldres_utdnivaa_var(
    df: pd.DataFrame,
    *,
    varname: str,
    merge_keys: list[str] | None,
) -> pd.DataFrame:
    """Generic DuckDB derivation helper for variables from `utd_foreldres_utdnivaa`."""
    df = df.copy()

    if varname in df.columns:
        logger.warning(f"{varname} already exists... Replacing it!")
        df = df.drop(columns=varname)

    merge_keys = merge_keys or []
    utd_foreldres_utdnivaa = NudbData("utd_foreldres_utdnivaa")

    con = nudb_database.get_connection()
    con.register("_tmp_df", df)

    result = con.sql(f"""
        SELECT
            T1.*,
            T2.{varname}
        FROM
            _tmp_df AS T1
        ASOF LEFT JOIN
            {utd_foreldres_utdnivaa.alias} AS T2
        ON
           T1.snr = T2.snr;
    """).df()

    if result.shape[0] > df.shape[0]:
        logger.warning(
            f"Number of observations grew from {df.shape[0]} to {result.shape[0]}!"
        )
    elif result.shape[0] < df.shape[0]:
        logger.warning(
            f"Number of observations decreased from {df.shape[0]} to {result.shape[0]}!"
        )

    return result


@wrap_derive
def utd_foreldres_utdnivaa_16aar_nus2000(df: pd.DataFrame) -> pd.DataFrame:
    """Derive `utd_foreldres_utdnivaa_16aar_nus2000`."""
    return _derive_utd_foreldres_utdnivaa_var(
        df,
        varname="utd_foreldres_utdnivaa_16aar_nus2000",
        merge_keys=(
            settings.variables.utd_foreldres_utdnivaa_16aar_nus2000.derived_join_keys
        ),
    )


@wrap_derive
def utd_hoeyeste_mor_nus2000(df: pd.DataFrame) -> pd.DataFrame:
    """Derive `utd_hoeyeste_mor_nus2000`."""
    return _derive_utd_foreldres_utdnivaa_var(
        df,
        varname="utd_hoeyeste_mor_nus2000",
        merge_keys=settings.variables.utd_hoeyeste_mor_nus2000.derived_join_keys,
    )


@wrap_derive
def utd_hoeyeste_far_nus2000(df: pd.DataFrame) -> pd.DataFrame:
    """Derive `utd_hoeyeste_far_nus2000`."""
    return _derive_utd_foreldres_utdnivaa_var(
        df,
        varname="utd_hoeyeste_far_nus2000",
        merge_keys=settings.variables.utd_hoeyeste_far_nus2000.derived_join_keys,
    )
