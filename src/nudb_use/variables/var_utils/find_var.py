"""Lookup helpers for NUDB variable metadata defined in config."""

from collections.abc import Iterable
from typing import Any

import klass
from nudb_config import settings

from nudb_use import logger


def find_vars(var_names: Iterable[str]) -> dict[str, dict]:
    """Look up multiple variables and return their configuration metadata.

    Args:
        var_names: Iterable of variable identifiers to resolve.

    Returns:
        dict[str, dict]: Mapping of requested names to their resolved metadata.
        Missing entries map to None.
    """
    result = {}
    for name in var_names:
        found_data = find_var(name)
        if found_data:
            logger.info(f"{name} -> {found_data['name']}")
            result[name] = found_data
        else:
            logger.warning(f"Couldnt find metadata for {name}")
            result[name] = None
    return result


def find_var(var_name: str) -> dict[str, Any] | None:
    """Retrieve configuration and KLASS metadata for a single variable.

    Args:
        var_name: Variable name (current or historical). Comparison is
            case-insensitive.

    Returns:
        dict[str, Any] | None: Dictionary with metadata when the variable is
        defined, otherwise None. The dictionary contains the variable configuration and metadata, including:
            - "name": The canonical variable name
            - All fields from the variable configuration
            - "klass_codelist_metadata": KlassClassification object if the variable
              has an associated codelist
            - "klass_variant_metadata": KlassVariant object if the variable has
              an associated variant
    """
    variables = settings.variables
    var_data = None
    if var_name.lower() in variables:
        var_data = {"name": var_name.lower()} | dict(
            settings.variables[var_name.lower()]
        )

    else:
        flip = {
            old_name: k
            for k, v in variables.items()
            if v.get("renamed_from")
            for old_name in v["renamed_from"]
        }
        if flip.get(var_name.lower()):
            var_data = {"name": flip.get(var_name.lower())} | dict(
                settings.variables[flip[var_name.lower()]]
            )

    # Get metadata from klass?
    if var_data:
        if var_data.get("klass_codelist"):
            var_data["klass_codelist_metadata"] = klass.KlassClassification(
                var_data["klass_codelist"]
            )
        if var_data.get("klass_variant"):
            var_data["klass_variant_metadata"] = klass.KlassVariant(
                var_data["klass_variant"]
            )
    # Get metadata from vardef?
    # Get metadata from datadoc?
    return var_data


def variables_missing_from_config(col_list: Iterable[str]) -> list[str]:
    """Identifies variables that are not defined in the settings.

    Args:
        col_list: An iterable of variable names to check against the settings.

    Returns:
        List of variable names that are not defined in the settings.
        Returns an empty list if all variables are found.
    """
    return [col for col in col_list if col not in settings.variables.keys()]
