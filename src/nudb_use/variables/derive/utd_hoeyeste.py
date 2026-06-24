import datetime as dt

import pandas as pd
from nudb_config import settings
from numpy import dtype as np_dtype
from numpy import generic as np_generic
from pandas.api.extensions import ExtensionDtype
from pandas.api.types import pandas_dtype

from nudb_use.datasets import NudbData
from nudb_use.datasets.nudb_database import nudb_database
from nudb_use.nudb_logger import logger
from nudb_use.variables.derive.derive_decorator import wrap_derive

__all__ = ["utd_hoeyeste_nus2000", "utd_hoeyeste_rangering"]


VENSTRESENSUR = settings.constants.venstresensur
INTEGER_DTYPE: ExtensionDtype | np_dtype[np_generic] = pandas_dtype(
    settings.constants.datadoc_pandas_dtype_mapping.INTEGER
)
STRING_DTYPE: ExtensionDtype | np_dtype[np_generic] = pandas_dtype(
    settings.constants.datadoc_pandas_dtype_mapping.STRING
)
VIDEREUTDANNING_UHGRUPPE = settings.constants.videreutd_uhgrupper


@wrap_derive
def utd_hoeyeste_rangering(df: pd.DataFrame) -> pd.Series:
    """Derive `utd_hoyeste_rangering`."""
    df = df.reset_index(drop=True).reset_index(
        names="__index_level_0"
    )  # drop twice in case of MultiIndex...
    con = nudb_database.get_connection()
    con.register("tmp_df_rangering", df)

    result = con.sql("""
        SELECT
            UTD_HOEYESTE_RANGERING(
                nus2000,
                uh_eksamen_dato,
                uh_eksamen_studpoeng,
                uh_gruppering_nus,
                utd_aktivitet_slutt,
                utd_klassetrinn,
                utd_skoleaar_start
        ) AS rangering

        FROM
            tmp_df_rangering

        ORDER BY
            __index_level_0 ASC;
    """).df()

    return result["rangering"]


@wrap_derive
def utd_hoeyeste_nus2000(df: pd.DataFrame, year_col: str | None = None) -> pd.Series:
    """Derive `utd_hoyeste_nus2000`."""
    df = df.copy()

    varname = "utd_hoeyeste_nus2000"
    if varname in df.columns:
        logger.warning(
            f'{varname} already exists... If `priority="old"` some values might not get updated!'
        )
        df = df.drop(columns=varname)

    year_col_right = "utd_hoeyeste_aar"
    if not year_col:
        year_col = year_col_right
        df[year_col] = dt.datetime.now().year

    df["snr"] = df["snr"].astype(STRING_DTYPE)
    df[year_col_right] = df[year_col].astype(INTEGER_DTYPE)

    utd_hoeyeste = NudbData("utd_hoeyeste")

    con = nudb_database.get_connection()
    con.register("_tmp_df", df[["snr", year_col_right]].drop_duplicates())

    mapping = con.sql(f"""
        SELECT DISTINCT
            T1.snr,
            T1.{year_col_right},
            T2.utd_hoeyeste_nus2000 AS {varname}
        FROM
            _tmp_df AS T1
        ASOF LEFT JOIN
            {utd_hoeyeste.alias} AS T2
        ON
            T1.snr = T2.snr AND
            T2.{year_col_right} <= T1.{year_col_right};
    """).df()

    result = df.merge(
        right=mapping, on=["snr", year_col_right], how="left", validate="m:1"
    )

    if result.shape[0] > df.shape[0]:
        logger.warning(
            f"Number of observations grew from {df.shape[0]} to {result.shape[0]}!"
        )
    elif result.shape[0] < df.shape[0]:
        logger.warning(
            f"Number of observations decreased from {df.shape[0]} to {result.shape[0]}!"
        )

    return result[varname]
