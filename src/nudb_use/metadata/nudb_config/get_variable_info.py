"""Helpers for reading NUDB variable metadata from configuration."""

from collections.abc import Mapping
from typing import Any

import pandas as pd
from nudb_config import settings as settings_use


def get_toml_field(toml: Mapping[str, Any], field: str) -> object | None:
    """Return a field from a TOML object or None if it is missing.

    Args:
        toml: Parsed TOML object.
        field: Field name to retrieve.

    Returns:
        object | None: Field value when present, otherwise None.
    """
    return None if field not in toml.keys() else toml[field]


def get_var_metadata(variables: list[str] | None = None) -> pd.DataFrame:
    """Get at pandas dataframe of the variable-data from the config.

    Args:
        variables: Variables to return data on, if None returns all variables.

    Returns:
        pd.DataFrame: The information from the metadata.
    """
    variables_list_of_dicts = [
        {"variable": var_name} | dict(settings_use.variables[var_name])
        for var_name in settings_use.variables.keys()
    ]
    df = pd.DataFrame(variables_list_of_dicts).set_index("variable")
    result: pd.DataFrame = df.loc[variables, :].copy() if variables else df
    return result
