import pandas as pd
from nudb_config import settings

from nudb_use.metadata.nudb_klass.correspondence import klass_correspondence_to_mapping

from .derive_decorator import wrap_derive


def _map_nus_correspondence(df: pd.DataFrame, varname: str) -> pd.Series:
    """Map `nus2000` through the KLASS correspondence defined for `varname`."""
    var_meta = settings.variables[varname]
    return df["nus2000"].map(klass_correspondence_to_mapping(var_meta))


@wrap_derive
def utd_isced2011_programmes_nus(  # noqa: DOC101,DOC103,DOC201,DOC203
    df: pd.DataFrame,
) -> pd.Series:
    """Derive utd_isced2011_programmes_nus from a correspondence on nus2000 -> ISCED programmes."""
    return _map_nus_correspondence(df, "utd_isced2011_programmes_nus")


@wrap_derive
def utd_isced2011_attainment_nus(  # noqa: DOC101,DOC103,DOC201,DOC203
    df: pd.DataFrame,
) -> pd.Series:
    """Derive utd_isced2011_attainment_nus from a correspondence on nus2000 -> ISCED attainment."""
    return _map_nus_correspondence(df, "utd_isced2011_attainment_nus")


@wrap_derive
def utd_isced2013_fagfelt_nus(  # noqa: DOC101,DOC103,DOC201,DOC203
    df: pd.DataFrame,
) -> pd.Series:
    """Derive utd_isced2013_fagfelt_nus from a correspondence on nus2000 -> ISCED fagfelt."""
    return _map_nus_correspondence(df, "utd_isced2013_fagfelt_nus")
