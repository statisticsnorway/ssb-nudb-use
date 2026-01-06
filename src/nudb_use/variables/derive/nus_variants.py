import pandas as pd

from .derive_decorator import wrap_derive


@wrap_derive
def utd_klassetrinn_lav_nus(  # noqa: DOC101,DOC103,DOC201,DOC203
    df: pd.DataFrame,
) -> pd.Series:
    """Derive utd_klassetrinn_lav_hoy_nus from nus2000."""
    utd_klassetrinn_lav_hoy_nus: pd.Series = df["utd_klassetrinn_lav_hoy_nus"]
    return utd_klassetrinn_lav_hoy_nus.str.split("-", n=1, expand=True)[0]


@wrap_derive
def utd_klassetrinn_hoy_nus(  # noqa: DOC101,DOC103,DOC201,DOC203
    df: pd.DataFrame,
) -> pd.Series:
    """Derive utd_klassetrinn_lav_hoy_nus from nus2000."""
    utd_klassetrinn_lav_hoy_nus: pd.Series = df["utd_klassetrinn_lav_hoy_nus"]
    return utd_klassetrinn_lav_hoy_nus.str.split("-", n=1, expand=True)[1]


@wrap_derive
def utd_erforeldet_kode_nus(  # noqa: DOC101,DOC103,DOC201,DOC203
    df: pd.DataFrame,
) -> pd.Series:
    """Derive utd_erforeldet_kode_nus from utd_foreldet_kode_nus nus2000."""
    return (df["utd_erforeldet_kode_nus"] == "*").astype("bool[pyarrow]")
