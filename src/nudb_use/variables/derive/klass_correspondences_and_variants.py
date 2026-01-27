from collections.abc import Callable

import pandas as pd
from nudb_config import settings

from nudb_use.metadata.nudb_klass.correspondence import klass_correspondence_to_mapping
from nudb_use.metadata.nudb_klass.variants import klass_variant_search_term_mapping
from nudb_use.nudb_logger import logger

from .derive_decorator import wrap_derive

module = __import__(__name__, fromlist=[""])  # pass non-empty fromlist
__all__ = []


def _map_klass_correspondence(
    df: pd.DataFrame, corresponds_to: str, varname: str
) -> pd.Series:
    """Map `corresponds_to` through the KLASS correspondence defined for `varname`."""
    var_meta = settings.variables[varname]
    return df[corresponds_to].map(klass_correspondence_to_mapping(var_meta))


def _map_klass_variant(df: pd.DataFrame, variant_of: str, varname: str) -> pd.Series:
    """Map `variant_of` through the KLASS variant defined for `varname`."""
    var_meta = settings.variables[varname]
    return df[variant_of].map(klass_variant_search_term_mapping(var_meta))


def _generate_klass_derive_function(
    varname: str,
) -> Callable[[pd.DataFrame], pd.DataFrame] | None:
    # we assume variable_label has the form <variable-name>_label
    var_meta = settings.variables[varname]

    is_variant = var_meta.klass_variant_search_term
    is_correspondence = var_meta.klass_correspondence_to
    derived_from = var_meta.derived_from
    is_relevant = is_variant or is_correspondence

    if not is_relevant or not derived_from:  # need to explicitly use `derived_from` for
        # mypy to understand that it is not None
        # So it is not used in `is_relevant`
        return None

    elif len(derived_from) > 1:
        logger.warning(f"""Don't know which variable to derive {varname} from!\n
                       as there are multiple options: {derived_from}""")
        return None
    elif is_variant:

        def basefunc(df: pd.DataFrame) -> pd.Series:
            return _map_klass_variant(df, variant_of=derived_from[0], varname=varname)

    elif is_correspondence:

        def basefunc(df: pd.DataFrame) -> pd.Series:
            return _map_klass_correspondence(
                df, corresponds_to=derived_from[0], varname=varname
            )

    basefunc.__doc__ = f"""Derive '{varname}' from '{derived_from}'."""
    basefunc.__name__ = varname

    return wrap_derive(basefunc)


for varname in settings.variables.keys():
    try:
        derivefunc = _generate_klass_derive_function(varname)

        if derivefunc:  # returns None if not applicable
            __all__.append(varname)
            setattr(module, varname, derivefunc)

    except Exception as err:
        logger.warning(
            f"Unable to generate derive function for '{varname}'!\nMessage: {err}"
        )
