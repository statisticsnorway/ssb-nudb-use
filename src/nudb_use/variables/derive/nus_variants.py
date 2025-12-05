import klass
import pandas as pd
from nudb_config import settings
from nudb_config.pydantic.variables import Variable

from .derive_decorator import wrap_derive


def klass_variant_nus_apply(df: pd.DataFrame, var_meta: Variable) -> pd.Series:
    """Subfunction to actually apply the codelist from the variant onto nus2000 and return the result.

    Args:
        df: The dataframe to get the nus2000 codes from to map from.
        var_meta: The variable metadata from the settings relating to the derived variable.

    Returns:
        pd.Series: The result of deriving a column based on nus2000 using the klass variant.
    """
    variant = (
        klass.KlassClassification(var_meta.klass_codelist)
        .get_version()  # Future development: Could we support "refdate" in the klass package on this to get the version by date?
        .get_variant(search_term=var_meta.klass_variant_search_term)
    )
    # Should we log the amount of nus2000 codes that do not map to a grouping in the variant?
    return df["nus2000"].map(variant.to_dict())


@wrap_derive
def utd_klassetrinn_lav_hoy_nus(df: pd.DataFrame) -> pd.Series:
    """Derive utd_klassetrinn_lav_hoy_nus from nus2000.

    # noqa: DAR101
    # noqa: DAR201
    """
    var_meta = settings.variables.utd_klassetrinn_lav_hoy_nus
    return klass_variant_nus_apply(df, var_meta)


@wrap_derive
def utd_historisk_foreldet_fag_nus(df: pd.DataFrame) -> pd.Series:
    """Derive utd_historisk_foreldet_fag_nus from nus2000.

    # noqa: DAR101
    # noqa: DAR201
    """
    var_meta = settings.variables.utd_historisk_foreldet_fag_nus
    return klass_variant_nus_apply(df, var_meta)
