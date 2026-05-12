import pandas as pd

from nudb_use import settings
from nudb_use.exceptions.exception_classes import NudbQualityError
from nudb_use.exceptions.groups import raise_exception_group
from nudb_use.exceptions.groups import warn_exception_group
from nudb_use.metadata.nudb_config.map_get_dtypes import DTYPE_MAPPINGS
from nudb_use.nudb_logger import LoggerStack
from nudb_use.nudb_logger import logger


def _check_dtype_column(x: pd.Series, name: str) -> NudbQualityError | None:
    # Is the variable defined in the config?
    if name not in settings.variables.keys():
        logger.warning(f"Unable to find '{name}' in ssb-nudb-config!")
        return None

    mappings = DTYPE_MAPPINGS["pandas"]
    metadata = settings.variables[name]
    dtype = metadata.dtype

    # Is the metadata valid?
    if dtype not in mappings.keys():
        logger.error(f"Unable to find pandas dtype for '{dtype}'!")
        return None

    target = mappings[dtype]
    have = x.dtype

    if have == target:
        logger.info(f"Variable '{name}' has the correct dtype :)")
        return None

    elif have != target:
        logger.warning(
            f"Variable '{name}' does not have the correct dtype (got={have}, want={target})"
        )

        try:
            x.astype(target)
            logger.info(f"Variable '{name}' can be casted to '{target}'")
            return None
        except Exception as err:
            return NudbQualityError(
                f"Variable could not be casted to the correct dtype ({target})! Message:\n{err}"
            )

    return None


def check_dtypes(
    df: pd.DataFrame,
    raise_errors: bool = True,
) -> list[NudbQualityError]:
    """Validate that the dtypes for columns are correct.

    Args:
        df: DataFrame to inspect.
        raise_errors: When True, raise grouped errors if mismatches are found.

    Returns:
        list[NudbQualityError]: Errors describing columns whose values are outside
        the allowed width definitions, or an empty list when all pass.
    """
    with LoggerStack(
        "Checking the widths of values in columns according to a dict sent in or gotten from the config."
    ):
        errors = []

        for col in df.columns:
            err = _check_dtype_column(df[col], name=col)

            if err:
                errors.append(err)

        if raise_errors:
            raise_exception_group(errors)
        else:
            warn_exception_group(errors)

        return errors
