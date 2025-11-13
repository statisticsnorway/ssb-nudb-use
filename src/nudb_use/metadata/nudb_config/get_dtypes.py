"""Utilities for mapping NUDB variable types to concrete dtype strings."""

from nudb_config import settings as SETTINGS

from nudb_use import logger
from nudb_use.metadata.nudb_config.get_variable_info import get_toml_field

MAPPINGS: dict[str, dict[str, str]] = {
    "pandas": {
        "DATETIME": "datetime64[s]",
        "STRING": "string[pyarrow]",
        "INTEGER": "Int64",
        "FLOAT": "Float64",
        "BOOLEAN": "bool[pyarrow]",
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
    if dtype not in mapping.keys():
        raise ValueError(f"Unkown type: {dtype}")
    result = mapping[dtype]
    logger.debug(f"First result from mapping: {result}")
    if datetimes_as_string and dtype.lower().startswith("datet"):
        k = next([x for x in mapping if x.lower().startswith("str")])
        result = mapping[k]
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
        KeyError: If `engine` is not defined in `MAPPINGS`.
    """
    if engine not in MAPPINGS:
        raise KeyError(
            f"Specify an engine in the mapping, or add to the mapping: {MAPPINGS.keys()}"
        )
    mapping = MAPPINGS[engine]
    return get_dtype_from_dict(
        dtype=dtype, mapping=mapping, datetimes_as_string=datetimes_as_string
    )


def get_dtypes(
    vars_map: list[str], engine: str = "pandas", datetimes_as_string: bool = False
) -> dict[str, str]:
    """Build a dtype mapping for a set of variables based on config metadata.

    Args:
        vars_map: Variable names to map, including historical names.
        engine: Mapping preset to use.
        datetimes_as_string: When True, convert datetime variables to string dtypes.

    Returns:
        dict[str, str]: Mapping of requested variables to dtype strings. Variables
        not found in config map to None.
    """
    conf_variables = SETTINGS["variables"]

    dtypes_want: dict[str, str] = {}
    renamed = {}
    for new in conf_variables.keys():
        old = get_toml_field(conf_variables[new], "renamed_from")
        if old is not None:
            if isinstance(old, str):
                renamed[old] = new
            else:
                for old_elem in old:
                    renamed[old_elem] = new

    # check for vars in 'renamed_from'?
    for var in vars_map:
        if var not in conf_variables.keys() and var not in renamed:
            logger.warning(
                f"Variable {var} not found, returning dtype=None!"
            )  # replace with logger later
            dtypes_want[var] = None
        elif var not in conf_variables.keys() and var in renamed:
            newname = renamed[var]
            logger.warning(f"Variables has been renamed from {var} to {newname}!")

            dtypes_want[var] = map_dtype_datadoc(
                dtype=conf_variables[newname]["dtype"],
                engine=engine,
                datetimes_as_string=datetimes_as_string,
            )
        else:
            dtypes_want[var] = map_dtype_datadoc(
                dtype=conf_variables[var]["dtype"],
                engine=engine,
                datetimes_as_string=datetimes_as_string,
            )

    return dtypes_want
