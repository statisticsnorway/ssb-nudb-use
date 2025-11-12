from nudb_config import settings
import klass
from typing import Iterable
from nudb_use import logger

def find_vars(var_names: str) -> dict[str, dict]:
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

def find_var(var_name: str):
    """Retrieves configuration and metadata for a variable by name.

    Args:
        var_name: The name of the variable to look up. Search is case-insensitive.
            Can be either the current variable name or a previous name if the
            variable has been renamed.
        
    Returns:
        var_data: Dictionary containing the variable configuration and metadata, including:
            - "name": The canonical variable name
            - All fields from the variable configuration
            - "klass_codelist_metadata": KlassClassification object if the variable
              has an associated codelist
            - "klass_variant_metadata": KlassVariant object if the variable has
              an associated variant
        
    Raises:
        KeyError: If the variable name is not found in either current variables
            or the renamed_from mapping.
    """
    variables = settings.variables
    var_data = None
    if var_name.lower() in variables:
        var_data = {"name": var_name.lower()} | dict(settings.variables[var_name.lower()])
        
    else:
        flip = {old_name: k for k, v in variables.items() if v.get("renamed_from") for old_name in v["renamed_from"]}
        if flip.get(var_name.lower()):
            var_data = {"name": flip.get(var_name.lower())} | dict(settings.variables[flip[var_name.lower()]])
        
    # Get metadata from klass?
    if var_data:
        if var_data.get("klass_codelist"):
            var_data["klass_codelist_metadata"] = klass.KlassClassification(var_data["klass_codelist"])
        if var_data.get("klass_variant"):
            var_data["klass_variant_metadata"] = klass.KlassVariant(var_data["klass_variant"])
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
