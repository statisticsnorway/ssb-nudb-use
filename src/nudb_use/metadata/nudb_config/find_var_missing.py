"""Lookup helpers for NUDB variable metadata defined in config."""

from collections.abc import Iterable
from typing import Any

import klass
from nudb_config import settings

from nudb_use.nudb_logger import logger

VariableMetadata = dict[str, Any]


def _normalize_variable(variable: Any) -> VariableMetadata:
    """Return a mutable dict representation of the provided variable metadata."""
    if isinstance(variable, dict):
        return dict(variable)
    if hasattr(variable, "model_dump"):
        return dict(variable.model_dump())
    if hasattr(variable, "dict"):
        return dict(variable.dict())
    return dict(vars(variable))


def _get_value(source: Any, key: str) -> Any:
    """Retrieve an attribute or mapping entry from a variable definition."""
    if isinstance(source, dict):
        return source.get(key)
    return getattr(source, key, None)


def find_vars(var_names: Iterable[str]) -> dict[str, VariableMetadata | None]:
    """Look up multiple variables and return their configuration metadata.

    Args:
        var_names: Iterable of variable identifiers to resolve.

    Returns:
        dict[str, VariableMetadata | None]: Mapping of requested names to their
        resolved metadata. Missing entries map to None.
    """
    result: dict[str, VariableMetadata | None] = {}
    for name in var_names:
        found_data = find_var(name)
        if found_data:
            logger.info(f"{name} -> {found_data['name']}")
            result[name] = found_data
        else:
            logger.warning(f"Couldnt find metadata for {name}")
            result[name] = None
    return result


def find_var(var_name: str) -> VariableMetadata | None:
    """Retrieve configuration and KLASS metadata for a single variable.

    Args:
        var_name: Variable name (current or historical). Comparison is
            case-insensitive.

    Returns:
        VariableMetadata | None:
    """
    variables = settings.variables
    var_data: VariableMetadata | None = None
    key = var_name.lower()
    if key in variables:
        var_data = _normalize_variable(variables[key])

    else:
        flip: dict[str, VariableMetadata] = {}
        for variable in variables.values():
            renamed_from = _get_value(variable, "renamed_from") or []
            for old_name in renamed_from:
                flip[old_name] = _normalize_variable(variable)
        if flip.get(key):
            logger.info(f"Column renamed {key} -> {flip.get(key)} - rename it?")
            var_data = flip[key]

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
        list[str]: Variable names that are not defined in the settings. Returns
        an empty list if all variables are found.
    """
    return [col for col in col_list if col not in settings.variables.keys()]
