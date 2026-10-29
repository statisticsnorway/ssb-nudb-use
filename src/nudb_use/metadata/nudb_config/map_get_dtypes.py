"""Utilities for mapping NUDB variable types to concrete dtype strings."""

from typing import Any
from typing import Final
from typing import Literal
from typing import TypeAlias
from typing import cast

import pandas as pd
from nudb_config import settings as SETTINGS

from nudb_use.nudb_logger import logger

from ._renames import RenameCandidates
from ._renames import _rename_candidates
from ._renames import _value

DTypeName: TypeAlias = Literal[
    "STRING", "DATETIME", "INTEGER", "FLOAT", "BOOLEAN", "UINTEGER"
]

STRING_DTYPE_NAME: Final[DTypeName] = "STRING"
DATETIME_DTYPE_NAME: Final[DTypeName] = "DATETIME"
INTEGER_DTYPE_NAME: Final[DTypeName] = "INTEGER"
FLOAT_DTYPE_NAME: Final[DTypeName] = "FLOAT"
BOOL_DTYPE_NAME: Final[DTypeName] = "BOOLEAN"
UINTEGER_DTYPE_NAME: Final[DTypeName] = "UINTEGER"

DATETIME_DTYPES: Final[set[DTypeName]] = {DATETIME_DTYPE_NAME}

PandasDTypeLiteral: TypeAlias = Literal[
    "datetime64[s]",
    "string[pyarrow]",
    "Int64",
    "Float64",
    "bool[pyarrow]",
    "UInt64",
]

BackendName: TypeAlias = Literal["pandas"]  # add others later when necessary

InnerMap: TypeAlias = dict[DTypeName, PandasDTypeLiteral]
MappingSpec: TypeAlias = dict[BackendName, InnerMap]

DTYPE_MAPPINGS: Final[MappingSpec] = {
    "pandas": {
        DATETIME_DTYPE_NAME: "datetime64[s]",
        STRING_DTYPE_NAME: "string[pyarrow]",
        INTEGER_DTYPE_NAME: "Int64",
        FLOAT_DTYPE_NAME: "Float64",
        BOOL_DTYPE_NAME: "bool[pyarrow]",
        UINTEGER_DTYPE_NAME: "UInt64",
    }
}

DTYPE_MAPPINGS_PANDAS = DTYPE_MAPPINGS["pandas"]


def get_dtype_from_dict(
    dtype: str,
    mapping: dict[str, str] | InnerMap,
    datetimes_as_string: bool = False,
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
    mapping_cast: InnerMap = cast(InnerMap, mapping)

    dtype_upper = (
        dtype.upper()
    )  # Make sure we have the same format as the names in the mapping
    if dtype_upper not in mapping_cast:
        raise ValueError(f"Unkown type: {dtype_upper}")

    dtype_key = dtype_upper
    result = mapping_cast[dtype_key]
    logger.debug(f"First result from mapping: {result}")

    if datetimes_as_string and dtype_key in DATETIME_DTYPES:
        result = mapping_cast[STRING_DTYPE_NAME]
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
    engine_key = engine
    mapping = DTYPE_MAPPINGS[engine_key]
    return get_dtype_from_dict(
        dtype=dtype, mapping=mapping, datetimes_as_string=datetimes_as_string
    )


def get_dtypes(
    vars_map: list[str],
    engine: str = "pandas",
    datetimes_as_string: bool = False,
    dataset_name: str | None = None,
) -> dict[str, str | None]:
    """Build a dtype mapping for a set of variables based on config metadata.

    Multiple rename targets are accepted when their configured dtypes agree.
    Conflicting dtypes raise ValueError with the targets and mapping sources.

    Args:
        vars_map: Variable names to map, including historical names.
        engine: Mapping preset to use.
        datetimes_as_string: When True, convert datetime variables to string dtypes.
        dataset_name: Dataset used to select rename overrides, or None for all mappings.

    Returns:
        dict[str, str | None]: Mapping of requested variables to dtype strings.
        Variables not found in config map to None.
    """
    conf_variables = {
        str(name).lower(): metadata for name, metadata in SETTINGS["variables"].items()
    }
    renamed = _rename_candidates(
        conf_variables, _value(SETTINGS, "datasets") or {}, dataset_name
    )

    return {
        var: _map_single_dtype(
            var, conf_variables, renamed, engine, datetimes_as_string, dataset_name
        )
        for var in vars_map
    }


def _map_single_dtype(
    var: str,
    conf_variables: dict[str, Any],
    renamed: RenameCandidates,
    engine: str,
    datetimes_as_string: bool,
    dataset_name: str | None,
) -> str | None:
    """Resolve dtype for a single variable, handling missing and renamed cases.

    Args:
        var: Requested column name.
        conf_variables: Current variable definitions keyed by lowercase name.
        renamed: Historical names mapped to targets and configuration sources.
        engine: Mapping preset to use.
        datetimes_as_string: Whether to map datetimes to string dtypes.
        dataset_name: Dataset used to select rename overrides.

    Returns:
        str | None: Mapped dtype, or None if the column is unknown.

    Raises:
        ValueError: If rename targets have different configured dtypes.
    """
    # An explicit dataset override takes precedence over an exact variable name.
    key = var.lower()
    matches = renamed.get(key, {})
    if key in conf_variables and not (dataset_name and matches):
        matches = {key: ["variables"]}
    if not matches:
        logger.warning(f"Variable {var} not found, returning dtype=None!")
        return None

    dtypes = {
        target: str(_value(conf_variables[target.lower()], "dtype")).upper()
        for target in matches
    }
    if len(set(dtypes.values())) > 1:
        details = "; ".join(
            f"{target!r}: {dtype} ({', '.join(matches[target])})"
            for target, dtype in dtypes.items()
        )
        raise ValueError(
            f"Cannot determine dtype for {var!r}: rename targets have different dtypes: {details}. "
            "Specify dataset_name or use a current variable name."
        )
    if key in renamed:
        logger.warning(f"Variable {var} has rename mappings: {matches}")

    return map_dtype_datadoc(
        dtype=next(iter(dtypes.values())),
        engine=engine,
        datetimes_as_string=datetimes_as_string,
    )


def map_to_preferred_dtypes(df: pd.DataFrame) -> dict[str, str]:
    """Get mapping from dataframe dtypes to preferred modern types that handle nullable cells better.

    - int64 -> Int64 (nullable integer)
    - float64 -> Float64 (nullable float)
    - bool -> boolean[pyarrow] (nullable boolean)
    - str, object, category, string -> string[pyarrow] (nullable string)
    - datetime64[ns] -> datetime64[s] (second precision)

    Args:
        df: The dataframe to set dtypes for.

    Returns:
        dict[str, str]: Dict with mapping from column names to new dtypes.
    """
    astype: dict[str, str] = {}

    for col in df.columns:
        dtype = df[col].dtype

        if pd.api.types.is_integer_dtype(dtype):
            astype[col] = str(DTYPE_MAPPINGS_PANDAS[INTEGER_DTYPE_NAME])

        elif pd.api.types.is_float_dtype(dtype):
            astype[col] = str(DTYPE_MAPPINGS_PANDAS[FLOAT_DTYPE_NAME])

        elif pd.api.types.is_bool_dtype(dtype):
            astype[col] = str(DTYPE_MAPPINGS_PANDAS[BOOL_DTYPE_NAME])

        elif (
            pd.api.types.is_string_dtype(dtype)
            or pd.api.types.is_object_dtype(dtype)
            or isinstance(
                dtype, pd.CategoricalDtype
            )  # pd.api.types.is_categorical_dtype() is deprecated
        ):
            astype[col] = str(DTYPE_MAPPINGS_PANDAS[STRING_DTYPE_NAME])

        elif pd.api.types.is_datetime64_any_dtype(dtype):
            astype[col] = str(DTYPE_MAPPINGS_PANDAS[DATETIME_DTYPE_NAME])

    return astype


def cast_to_preferred_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """Cast dataframe dtypes to preferred modern types that handle nullable cells better.

    - int64 -> Int64 (nullable integer)
    - float64 -> Float64 (nullable float)
    - bool -> boolean[pyarrow] (nullable boolean)
    - str, object, category, string -> string[pyarrow] (nullable string)
    - datetime64[ns] -> datetime64[s] (second precision)

    Args:
        df: The dataframe to set dtypes for.

    Returns:
        pd.DataFrame: The modified pandas dataframe.
    """
    return df.astype(map_to_preferred_dtypes(df))
