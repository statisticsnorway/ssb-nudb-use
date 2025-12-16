"""Utilities for mapping NUDB variable types to concrete dtype strings."""

from typing import Any

from nudb_config import settings as SETTINGS

from nudb_use.nudb_logger import logger

# Global variables to refer to names programatically
# in case we want to change names later
STRING_DTYPE_NAME = "STRING"
DATETIME_DTYPE_NAME = "DATETIME"
INTEGER_DTYPE_NAME = "INTEGER"
FLOAT_DTYPE_NAME = "FLOAT"
BOOL_DTYPE_NAME = "BOOLEAN"
DATETIME_DTYPES = {DATETIME_DTYPE_NAME}

DTYPE_MAPPINGS: dict[str, dict[str, str]] = {
    "pandas": {
        DATETIME_DTYPE_NAME: "datetime64[s]",
        STRING_DTYPE_NAME: "string[pyarrow]",
        INTEGER_DTYPE_NAME: "Int64",
        FLOAT_DTYPE_NAME: "Float64",
        BOOL_DTYPE_NAME: "bool[pyarrow]",
    }
}


def get_dtype_from_dict(
    dtype: str, mapping: dict[str, str], datetimes_as_string: bool = False
) -> str:
    """Resolve a dtype string through a mapping with optional datetime override.

    Args:
        dtype: Logical dtype name from configuration.
        mapping: Mapping of logical names to engine-specific dtype strings.
        datetimes_as_string: When True, map datetime fields to string equivalents.

    Returns:
        str: Engine-specific dtype string.

    Raises:
        ValueError: If `dtype` is not defined in `mapping`.
    """
    dtype = (
        dtype.upper()
    )  # Make sure we have the same format as the names in the mapping
    if dtype not in mapping.keys():
        raise ValueError(f"Unkown type: {dtype}")

    result = mapping[dtype]
    logger.debug(f"First result from mapping: {result}")

    if datetimes_as_string and dtype in DATETIME_DTYPES:
        result = mapping[STRING_DTYPE_NAME]
        logger.debug(f"Second result from mapping: {result}")

    return result


def map_dtype_datadoc(
    dtype: str, engine: str = "pandas", datetimes_as_string: bool = False
) -> str:
    """Map a logical dtype using a named engine mapping.

    Args:
        dtype: Logical dtype value from the NUDB config.
        engine: Name of the mapping preset to use.
        datetimes_as_string: When True, map datetimes to string dtypes.

    Returns:
        str: Concrete dtype string for the target engine.

    Raises:
        KeyError: If `engine` is not defined in `DTYPE_MAPPINGS`.
    """
    if engine not in DTYPE_MAPPINGS:
        raise KeyError(
            f"Specify an engine in the mapping, or add to the mapping: {DTYPE_MAPPINGS.keys()}"
        )
    mapping = DTYPE_MAPPINGS[engine]
    return get_dtype_from_dict(
        dtype=dtype, mapping=mapping, datetimes_as_string=datetimes_as_string
    )


def get_dtypes(
    vars_map: list[str], engine: str = "pandas", datetimes_as_string: bool = False
) -> dict[str, str | None]:
    """Build a dtype mapping for a set of variables based on config metadata.

    Args:
        vars_map: Variable names to map, including historical names.
        engine: Mapping preset to use.
        datetimes_as_string: When True, convert datetime variables to string dtypes.

    Returns:
        dict[str, str | None]: Mapping of requested variables to dtype strings.
        Variables not found in config map to None.
    """
    conf_variables = SETTINGS["variables"]
    renamed = _build_renamed_lookup(conf_variables)

    return {
        var: _map_single_dtype(
            var, conf_variables, renamed, engine, datetimes_as_string
        )
        for var in vars_map
    }


def _build_renamed_lookup(conf_variables: dict[str, Any]) -> dict[str, str]:
    """Build a lookup from historical names to current names."""
    renamed: dict[str, str] = {}
    for new, meta in conf_variables.items():
        old: list[str] | str | None = meta.renamed_from
        if old is None or old == []:
            continue
        if isinstance(old, str):
            renamed[old] = new
        else:
            for old_elem in old:
                renamed[old_elem] = new
    return renamed


def _map_single_dtype(
    var: str,
    conf_variables: dict[str, Any],
    renamed: dict[str, str],
    engine: str,
    datetimes_as_string: bool,
) -> str | None:
    """Resolve dtype for a single variable, handling missing and renamed cases."""
    if var not in conf_variables and var not in renamed:
        logger.warning(f"Variable {var} not found, returning dtype=None!")
        return None

    target = renamed.get(var, var)
    if var in renamed:
        logger.warning(f"Variables has been renamed from {var} to {target}!")

    return map_dtype_datadoc(
        dtype=conf_variables[target]["dtype"],
        engine=engine,
        datetimes_as_string=datetimes_as_string,
    )
