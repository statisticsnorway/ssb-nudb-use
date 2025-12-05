import pandas as pd
from nudb_config import settings

from .derive_decorator import wrap_derive
from .klass_derive_utils import klass_variant_search_term_mapping


@wrap_derive
def utd_klassetrinn_lav_hoy_nus(df: pd.DataFrame) -> pd.Series:
    """Derive utd_klassetrinn_lav_hoy_nus from nus2000.

    # noqa: DAR101
    # noqa: DAR201
    """
    var_meta = settings.variables.utd_klassetrinn_lav_hoy_nus
    return df["nus2000"].map(klass_variant_search_term_mapping(var_meta))


@wrap_derive
def utd_historisk_foreldet_fag_nus(df: pd.DataFrame) -> pd.Series:
    """Derive utd_historisk_foreldet_fag_nus from nus2000.

    # noqa: DAR101
    # noqa: DAR201
    """
    var_meta = settings.variables.utd_historisk_foreldet_fag_nus
    return df["nus2000"].map(klass_variant_search_term_mapping(var_meta))
