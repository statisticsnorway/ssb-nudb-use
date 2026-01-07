from collections.abc import Callable

import pandas as pd
from nudb_config import settings

from nudb_use.metadata.nudb_klass.labels import get_klass_label_mapping
from nudb_use.nudb_logger import logger
from nudb_use.variables.derive.derive_decorator import wrap_derive

module = __import__(__name__, fromlist=[""])  # pass non-empty fromlist
__all__ = []


class MissingLabelMappingError(Exception): ...


def _generate_label_function(
    variable_label: str,
) -> Callable[[pd.DataFrame], pd.DataFrame] | None:
    # we assume variable_label has the form <variable-name>_label
    variable = variable_label.removesuffix("_label")

    def basefunc(df: pd.DataFrame) -> pd.Series:
        mapping = get_klass_label_mapping(variable)

        if mapping:
            return df[variable].map(mapping)
        else:
            raise MissingLabelMappingError(
                f"Unable to generate label mapping for '{variable}'"
            )

    basefunc.__doc__ = f"""Derive {variable_label}, with klass labels for {variable}."""
    basefunc.__name__ = variable_label

    return wrap_derive(basefunc)


confvars: list[str] = list(settings.variables.keys())
labvars: list[str] = [var for var in confvars if var.endswith("_label")]

for labvar in labvars:
    try:
        labfunc = _generate_label_function(labvar)
        __all__.append(labvar)
        setattr(module, labvar, labfunc)
    except Exception as err:
        logger.warning(
            f"Unable to generate derive function for '{labvar}'!\nMessage: {err}"
        )
