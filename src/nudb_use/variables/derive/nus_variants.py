import pandas as pd
import polars as pl

from .derive_decorator import wrap_derive


@wrap_derive
def utd_klassetrinn_lav_nus(  # noqa:DOC201
    _lf: pl.LazyFrame,
) -> pl.Series:
    """Derive utd_klassetrinn_lav_hoey_nus from nus2000."""
    utd_klassetrinn_lav_hoey_nus: pl.Expr = pl.col("utd_klassetrinn_lav_hoey_nus")
    return utd_klassetrinn_lav_hoey_nus.str.split_exact("-", 1).struct.field("field_0")


@wrap_derive
def utd_klassetrinn_hoey_nus(  # noqa:DOC201
    df: pd.DataFrame,
) -> pd.Series:
    """Derive utd_klassetrinn_lav_hoey_nus from nus2000."""
    utd_klassetrinn_lav_hoey_nus: pl.Expr = pl.col("utd_klassetrinn_lav_hoey_nus")
    return utd_klassetrinn_lav_hoey_nus.str.split_exact("-", 1).struct.field("field_1")


@wrap_derive
def utd_erforeldet_kode_nus(  # noqa:DOC201
    df: pd.DataFrame,
) -> pd.Series:
    """Derive utd_erforeldet_kode_nus from utd_foreldet_kode_nus nus2000."""
    return (df["utd_foreldet_kode_nus"] == "*").astype("bool[pyarrow]")
